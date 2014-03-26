package hex.gbm;

import static hex.gbm.SharedTreeModelBuilder.createRNG;
import hex.*;
import hex.ConfusionMatrix;
import hex.gbm.DTree.TreeModel.CompressedTree;
import hex.gbm.DTree.TreeModel.TreeVisitor;

import java.util.*;

import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.Chunk;
import water.util.*;

/**
   A Decision Tree, laid over a Frame of Vecs, and built distributed.

   This class defines an explicit Tree structure, as a collection of {@code DTree}
   {@code Node}s.  The Nodes are numbered with a unique {@code _nid}.  Users
   need to maintain their own mapping from their data to a {@code _nid}, where
   the obvious technique is to have a Vec of {@code _nid}s (ints), one per each
   element of the data Vecs.

   Each {@code Node} has a {@code DHistogram}, describing summary data about the
   rows.  The DHistogram requires a pass over the data to be filled in, and we
   expect to fill in all rows for Nodes at the same depth at the same time.
   i.e., a single pass over the data will fill in all leaf Nodes' DHistograms
   at once.

   @author Cliff Click
*/
public class DTree extends Iced {
  final String[] _names; // Column names
  final int _ncols;      // Active training columns
  final char _nbins;     // Max number of bins to split over
  final char _nclass;    // #classes, or 1 for regression trees
  final int _min_rows;   // Fewest allowed rows in any split
  final long _seed;      // RNG seed; drives sampling seeds if necessary
  private Node[] _ns;    // All the nodes in the tree.  Node 0 is the root.
  int _len;              // Resizable array
  public DTree( String[] names, int ncols, char nbins, char nclass, int min_rows ) { this(names,ncols,nbins,nclass,min_rows,-1); }
  public DTree( String[] names, int ncols, char nbins, char nclass, int min_rows, long seed ) {
    _names = names; _ncols = ncols; _nbins=nbins; _nclass=nclass; _min_rows = min_rows; _ns = new Node[1]; _seed = seed; }

  public final Node root() { return _ns[0]; }
  // One-time local init after wire transfer
  void init_tree( ) { for( int j=0; j<_len; j++ ) _ns[j]._tree = this; }

  // Return Node i
  public final Node node( int i ) {
    if( i >= _len ) throw new ArrayIndexOutOfBoundsException(i);
    return _ns[i];
  }
  public final UndecidedNode undecided( int i ) { return (UndecidedNode)node(i); }
  public final   DecidedNode   decided( int i ) { return (  DecidedNode)node(i); }

  // Get a new node index, growing innards on demand
  private synchronized int newIdx(Node n) {
    if( _len == _ns.length ) _ns = Arrays.copyOf(_ns,_len<<1);
    _ns[_len] = n;
    return _len++;
  }
  // Return a deterministic chunk-local RNG.  Can be kinda expensive.
  // Override this in, e.g. Random Forest algos, to get a per-chunk RNG
  public Random rngForChunk( int cidx ) { throw H2O.fail(); }

  public final int len() { return _len; }
  public final void len(int len) { _len = len; }

  // Public stats about tree
  public int leaves;
  public int depth;

  // --------------------------------------------------------------------------
  // Abstract node flavor
  public static abstract class Node extends Iced {
    transient protected DTree _tree;    // Make transient, lest we clone the whole tree
    final protected int _pid;           // Parent node id, root has no parent and uses -1
    final protected int _nid;           // My node-ID, 0 is root
    Node( DTree tree, int pid, int nid ) {
      _tree = tree;
      _pid=pid;
      tree._ns[_nid=nid] = this;
    }
    Node( DTree tree, int pid ) {
      _tree = tree;
      _pid=pid;
      _nid = tree.newIdx(this);
    }

    // Recursively print the decision-line from tree root to this child.
    StringBuilder printLine(StringBuilder sb ) {
      if( _pid==-1 ) return sb.append("[root]");
      DecidedNode parent = _tree.decided(_pid);
      parent.printLine(sb).append(" to ");
      return parent.printChild(sb,_nid);
    }
    abstract public StringBuilder toString2(StringBuilder sb, int depth);
    abstract protected AutoBuffer compress(AutoBuffer ab);
    abstract protected int size();

    public final int nid() { return _nid; }
    public final int pid() { return _pid; }
  }

  // --------------------------------------------------------------------------
  // Records a column, a bin to split at within the column, and the MSE.
  public static class Split extends Iced {
    final int _col, _bin;       // Column to split, bin where being split
    final boolean _equal;       // Split is < or == ?
    final double _se0, _se1;    // Squared error of each subsplit
    final long    _n0,  _n1;    // Rows in each final split
    final double  _p0,  _p1;    // Predicted value for each split

    public Split( int col, int bin, boolean equal, double se0, double se1, long n0, long n1, double p0, double p1 ) {
      _col = col;  _bin = bin;  _equal = equal;
      _n0 = n0;  _n1 = n1;  _se0 = se0;  _se1 = se1;
      _p0 = p0;  _p1 = p1;
    }
    public final double se() { return _se0+_se1; }
    public final int   col() { return _col; }
    public final int   bin() { return _bin; }
    public final long  rowsLeft () { return _n0; }
    public final long  rowsRight() { return _n1; }
    /** Returns empirical improvement in mean-squared error.
     *
     *  Formula for node splittin space into two subregions R1,R2 with predictions y1, y2:
     *    i2(R1,R2) ~ w1*w2 / (w1+w2) * (y1 - y2)^2
     *
     * @see (35), (45) in J. Friedman - Greedy Function Approximation: A Gradient boosting machine */
    public final float improvement() {
      double d = (_p0-_p1);
      return (float) ( d*d*_n0*_n1 / (_n0+_n1) );
    }

    // Split-at dividing point.  Don't use the step*bin+bmin, due to roundoff
    // error we can have that point be slightly higher or lower than the bin
    // min/max - which would allow values outside the stated bin-range into the
    // split sub-bins.  Always go for a value which splits the nearest two
    // elements.
    float splat(DHistogram hs[]) {
      DHistogram h = hs[_col];
      assert _bin > 0 && _bin < h.nbins();
      if( _equal ) { assert h.bins(_bin)!=0; return h.binAt(_bin); }
      // Find highest non-empty bin below the split
      int x=_bin-1;
      while( x >= 0 && h.bins(x)==0 ) x--;
      // Find lowest  non-empty bin above the split
      int n=_bin;
      while( n < h.nbins() && h.bins(n)==0 ) n++;
      // Lo is the high-side of the low non-empty bin, rounded to int for int columns
      // Hi is the low -side of the hi  non-empty bin, rounded to int for int columns

      // Example: Suppose there are no empty bins, and we are splitting an
      // integer column at 48.4 (more than nbins, so step != 1.0, perhaps
      // step==1.8).  The next lowest non-empty bin is from 46.6 to 48.4, and
      // we set lo=48.4.  The next highest non-empty bin is from 48.4 to 50.2
      // and we set hi=48.4.  Since this is an integer column, we round lo to
      // 48 (largest integer below the split) and hi to 49 (smallest integer
      // above the split).  Finally we average them, and split at 48.5.
      float lo = h.binAt(x+1);
      float hi = h.binAt(n  );
      if( h._isInt > 0 ) lo = h._step==1 ? lo-1 : (float)Math.floor(lo);
      if( h._isInt > 0 ) hi = h._step==1 ? hi   : (float)Math.ceil (hi);
      return (lo+hi)/2.0f;
    }

    // Split a DHistogram.  Return null if there is no point in splitting
    // this bin further (such as there's fewer than min_row elements, or zero
    // error in the response column).  Return an array of DHistograms (one
    // per column), which are bounded by the split bin-limits.  If the column
    // has constant data, or was not being tracked by a prior DHistogram
    // (for being constant data from a prior split), then that column will be
    // null in the returned array.
    public DHistogram[] split( int way, char nbins, int min_rows, DHistogram hs[], float splat ) {
      long n = way==0 ? _n0 : _n1;
      if( n < min_rows || n <= 1 ) return null; // Too few elements
      double se = way==0 ? _se0 : _se1;
      if( se <= 1e-30 ) return null; // No point in splitting a perfect prediction

      // Build a next-gen split point from the splitting bin
      int cnt=0;                  // Count of possible splits
      DHistogram nhists[] = new DHistogram[hs.length]; // A new histogram set
      for( int j=0; j<hs.length; j++ ) { // For every column in the new split
        DHistogram h = hs[j];            // old histogram of column
        if( h == null ) continue;        // Column was not being tracked?
        // min & max come from the original column data, since splitting on an
        // unrelated column will not change the j'th columns min/max.
        // Tighten min/max based on actual observed data for tracked columns
        float min, maxEx;
        if( h._bins == null ) { // Not tracked this last pass?
          min = h._min;         // Then no improvement over last go
          maxEx = h._maxEx;
        } else {                // Else pick up tighter observed bounds
          min = h.find_min();   // Tracked inclusive lower bound
          if( h.find_maxIn() == min ) continue; // This column will not split again
          maxEx = h.find_maxEx(); // Exclusive max
        }

        // Tighter bounds on the column getting split: exactly each new
        // DHistogram's bound are the bins' min & max.
        if( _col==j ) {
          if( _equal ) {        // Equality split; no change on unequals-side
            if( way == 1 ) continue; // but know exact bounds on equals-side - and this col will not split again
          } else {              // Less-than split
            if( h._bins[_bin]==0 )
              throw H2O.unimpl(); // Here I should walk up & down same as split() above.
            float split = splat;
            if( h._isInt > 0 ) split = (float)Math.ceil(split);
            if( way == 0 ) maxEx= split;
            else           min  = split;
          }
        }
        if( Utils.equalsWithinOneSmallUlp(min, maxEx) ) continue; // This column will not split again
        if( h._isInt > 0 && !(min+1 < maxEx ) ) continue; // This column will not split again
        if( min >  maxEx ) continue; // Happens for all-NA subsplits
        assert min < maxEx && n > 1 : ""+min+"<"+maxEx+" n="+n;
        nhists[j] = DHistogram.make(h._name,nbins,h._isInt,min,maxEx,n,h.isBinom());
        cnt++;                    // At least some chance of splitting
      }
      return cnt == 0 ? null : nhists;
    }

    public static StringBuilder ary2str( StringBuilder sb, int w, long xs[] ) {
      sb.append('[');
      for( long x : xs ) UndecidedNode.p(sb,x,w).append(",");
      return sb.append(']');
    }
    public static StringBuilder ary2str( StringBuilder sb, int w, float xs[] ) {
      sb.append('[');
      for( float x : xs ) UndecidedNode.p(sb,x,w).append(",");
      return sb.append(']');
    }
    public static StringBuilder ary2str( StringBuilder sb, int w, double xs[] ) {
      sb.append('[');
      for( double x : xs ) UndecidedNode.p(sb,(float)x,w).append(",");
      return sb.append(']');
    }
    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{"+_col+"/");
      UndecidedNode.p(sb,_bin,2);
      sb.append(", se0=").append(_se0);
      sb.append(", se1=").append(_se1);
      sb.append(", n0=" ).append(_n0 );
      sb.append(", n1=" ).append(_n1 );
      return sb.append("}").toString();
    }
  }

  // --------------------------------------------------------------------------
  // An UndecidedNode: Has a DHistogram which is filled in (in parallel
  // with other histograms) in a single pass over the data.  Does not contain
  // any split-decision.
  public static abstract class UndecidedNode extends Node {
    public transient DHistogram[] _hs;
    public final int _scoreCols[];      // A list of columns to score; could be null for all
    public UndecidedNode( DTree tree, int pid, DHistogram[] hs ) {
      super(tree,pid);
      assert hs.length==tree._ncols;
      _scoreCols = scoreCols(_hs=hs);
    }

    // Pick a random selection of columns to compute best score.
    // Can return null for 'all columns'.
    abstract public int[] scoreCols( DHistogram[] hs );

    // Make the parent of this Node use a -1 NID to prevent the split that this
    // node otherwise induces.  Happens if we find out too-late that we have a
    // perfect prediction here, and we want to turn into a leaf.
    public void do_not_split( ) {
      if( _pid == -1 ) return;
      DecidedNode dn = _tree.decided(_pid);
      for( int i=0; i<dn._nids.length; i++ )
        if( dn._nids[i]==_nid )
          { dn._nids[i] = -1; return; }
      throw H2O.fail();
    }

    @Override public String toString() {
      final int nclass = _tree._nclass;
      final String colPad="  ";
      final int cntW=4, mmmW=4, menW=5, varW=5;
      final int colW=cntW+1+mmmW+1+mmmW+1+menW+1+varW;
      StringBuilder sb = new StringBuilder();
      sb.append("Nid# ").append(_nid).append(", ");
      printLine(sb).append("\n");
      if( _hs == null ) return sb.append("_hs==null").toString();
      final int ncols = _hs.length;
      for( int j=0; j<ncols; j++ )
        if( _hs[j] != null )
          p(sb,_hs[j]._name+String.format(", %4.1f-%4.1f",_hs[j]._min,_hs[j]._maxEx),colW).append(colPad);
      sb.append('\n');
      for( int j=0; j<ncols; j++ ) {
        if( _hs[j] == null ) continue;
        p(sb,"cnt" ,cntW).append('/');
        p(sb,"min" ,mmmW).append('/');
        p(sb,"max" ,mmmW).append('/');
        p(sb,"mean",menW).append('/');
        p(sb,"var" ,varW).append(colPad);
      }
      sb.append('\n');

      // Max bins
      int nbins=0;
      for( int j=0; j<ncols; j++ )
        if( _hs[j] != null && _hs[j].nbins() > nbins ) nbins = _hs[j].nbins();

      for( int i=0; i<nbins; i++ ) {
        for( int j=0; j<ncols; j++ ) {
          DHistogram h = _hs[j];
          if( h == null ) continue;
          if( i < h.nbins() && h._bins != null ) {
            p(sb, h.bins(i),cntW).append('/');
            p(sb, h.binAt(i),mmmW).append('/');
            p(sb, h.binAt(i+1),mmmW).append('/');
            p(sb, h.mean(i),menW).append('/');
            p(sb, h.var (i),varW).append(colPad);
          } else {
            p(sb,"",colW).append(colPad);
          }
        }
        sb.append('\n');
      }
      sb.append("Nid# ").append(_nid);
      return sb.toString();
    }
    static private StringBuilder p(StringBuilder sb, String s, int w) {
      return sb.append(Log.fixedLength(s,w));
    }
    static private StringBuilder p(StringBuilder sb, long l, int w) {
      return p(sb,Long.toString(l),w);
    }
    static private StringBuilder p(StringBuilder sb, double d, int w) {
      String s = Double.isNaN(d) ? "NaN" :
        ((d==Float.MAX_VALUE || d==-Float.MAX_VALUE || d==Double.MAX_VALUE || d==-Double.MAX_VALUE) ? " -" :
         (d==0?" 0":Double.toString(d)));
      if( s.length() <= w ) return p(sb,s,w);
      s = String.format("% 4.2f",d);
      if( s.length() > w )
        s = String.format("%4.1f",d);
      if( s.length() > w )
        s = String.format("%4.0f",d);
      return p(sb,s,w);
    }

    @Override public StringBuilder toString2(StringBuilder sb, int depth) {
      for( int d=0; d<depth; d++ ) sb.append("  ");
      return sb.append("Undecided\n");
    }
    @Override protected AutoBuffer compress(AutoBuffer ab) { throw H2O.fail(); }
    @Override protected int size() { throw H2O.fail(); }
  }

  // --------------------------------------------------------------------------
  // Internal tree nodes which split into several children over a single
  // column.  Includes a split-decision: which child does this Row belong to?
  // Does not contain a histogram describing how the decision was made.
  public static abstract class DecidedNode extends Node {
    public final Split _split;         // Split: col, equal/notequal/less/greater, nrows, MSE
    public final float _splat;         // Split At point: lower bin-edge of split
    // _equals\_nids[] \   0   1
    // ----------------+----------
    //       F         |   <   >=
    //       T         |  !=   ==
    public final int _nids[];          // Children NIDS for the split LEFT, RIGHT

    transient byte _nodeType; // Complex encoding: see the compressed struct comments
    transient int _size = 0;  // Compressed byte size of this subtree

    // Make a correctly flavored Undecided
    public abstract UndecidedNode makeUndecidedNode(DHistogram hs[]);

    // Pick the best column from the given histograms
    public abstract Split bestCol( UndecidedNode u, DHistogram hs[] );

    public DecidedNode( UndecidedNode n, DHistogram hs[] ) {
      super(n._tree,n._pid,n._nid); // Replace Undecided with this DecidedNode
      _nids = new int[2];           // Split into 2 subsets
      _split = bestCol(n,hs);       // Best split-point for this tree
      if( _split._col == -1 ) {     // No good split?
        // Happens because the predictor columns cannot split the responses -
        // which might be because all predictor columns are now constant, or
        // because all responses are now constant.
        _splat = Float.NaN;
        Arrays.fill(_nids,-1);
        return;
      }
      _splat = _split.splat(hs); // Split-at value
      final char nbins   = _tree._nbins;
      final int min_rows = _tree._min_rows;

      for( int b=0; b<2; b++ ) { // For all split-points
        // Setup for children splits
        DHistogram nhists[] = _split.split(b,nbins,min_rows,hs,_splat);
        assert nhists==null || nhists.length==_tree._ncols;
        _nids[b] = nhists == null ? -1 : makeUndecidedNode(nhists)._nid;
      }
    }

    // Bin #.
    public int bin( Chunk chks[], int row ) {
      float d = (float)chks[_split._col].at0(row); // Value to split on for this row
      if( Float.isNaN(d) )               // Missing data?
        return 0;                        // NAs always to bin 0
      // Note that during *scoring* (as opposed to training), we can be exposed
      // to data which is outside the bin limits.
      return _split._equal ? (d != _splat ? 0 : 1) : (d < _splat ? 0 : 1);
    }

    public int ns( Chunk chks[], int row ) { return _nids[bin(chks,row)]; }

    public double pred( int nid ) { return nid==0 ? _split._p0 : _split._p1; }

    @Override public String toString() {
      if( _split._col == -1 ) return "Decided has col = -1";
      int col = _split._col;
      if( _split._equal )
        return
          _tree._names[col]+" != "+_splat+"\n"+
          _tree._names[col]+" == "+_splat+"\n";
      return
        _tree._names[col]+" < "+_splat+"\n"+
        _splat+" <="+_tree._names[col]+"\n";
    }

    StringBuilder printChild( StringBuilder sb, int nid ) {
      int i = _nids[0]==nid ? 0 : 1;
      assert _nids[i]==nid : "No child nid "+nid+"? " +Arrays.toString(_nids);
      sb.append("[").append(_tree._names[_split._col]);
      sb.append(_split._equal
                ? (i==0 ? " != " : " == ")
                : (i==0 ? " <  " : " >= "));
      sb.append(_splat).append("]");
      return sb;
    }

    @Override public StringBuilder toString2(StringBuilder sb, int depth) {
      for( int i=0; i<_nids.length; i++ ) {
        for( int d=0; d<depth; d++ ) sb.append("  ");
        sb.append(_nid).append(" ");
        if( _split._col < 0 ) sb.append("init");
        else {
          sb.append(_tree._names[_split._col]);
          sb.append(_split._equal
                    ? (i==0 ? " != " : " == ")
                    : (i==0 ? " <  " : " >= "));
          sb.append(_splat).append("\n");
        }
        if( _nids[i] >= 0 && _nids[i] < _tree._len )
          _tree.node(_nids[i]).toString2(sb,depth+1);
      }
      return sb;
    }

    // Size of this subtree; sets _nodeType also
    @Override public final int size(){
      if( _size != 0 ) return _size; // Cached size

      assert _nodeType == 0:"unexpected node type: " + _nodeType;
      if( _split._equal ) _nodeType |= (byte)4;

      int res = 7; // 1B node type + flags, 2B colId, 4B float split val

      Node left = _tree.node(_nids[0]);
      int lsz = left.size();
      res += lsz;
      if( left instanceof LeafNode ) _nodeType |= (byte)(24 << 0*2);
      else {
        int slen = lsz < 256 ? 0 : (lsz < 65535 ? 1 : (lsz<(1<<24) ? 2 : 3));
        _nodeType |= slen; // Set the size-skip bits
        res += (slen+1); //
      }

      Node rite = _tree.node(_nids[1]);
      if( rite instanceof LeafNode ) _nodeType |= (byte)(24 << 1*2);
      res += rite.size();
      assert (_nodeType&0x1B) != 27;
      assert res != 0;
      return (_size = res);
    }

    // Compress this tree into the AutoBuffer
    @Override public AutoBuffer compress(AutoBuffer ab) {
      int pos = ab.position();
      if( _nodeType == 0 ) size(); // Sets _nodeType & _size both
      ab.put1(_nodeType);          // Includes left-child skip-size bits
      assert _split._col != -1;    // Not a broken root non-decision?
      ab.put2((short)_split._col);
      ab.put4f(_splat);
      Node left = _tree.node(_nids[0]);
      if( (_nodeType&24) == 0 ) { // Size bits are optional for left leaves !
        int sz = left.size();
        if(sz < 256)            ab.put1(       sz);
        else if (sz < 65535)    ab.put2((short)sz);
        else if (sz < (1<<24))  ab.put3(       sz);
        else                    ab.put4(       sz); // 1<<31-1
      }
      // now write the subtree in
      left.compress(ab);
      Node rite = _tree.node(_nids[1]);
      rite.compress(ab);
      assert _size == ab.position()-pos:"reported size = " + _size + " , real size = " + (ab.position()-pos);
      return ab;
    }
  }

  public static abstract class LeafNode extends Node {
    public double _pred;
    public LeafNode( DTree tree, int pid ) { super(tree,pid); }
    public LeafNode( DTree tree, int pid, int nid ) { super(tree,pid,nid); }
    @Override public String toString() { return "Leaf#"+_nid+" = "+_pred; }
    @Override public final StringBuilder toString2(StringBuilder sb, int depth) {
      for( int d=0; d<depth; d++ ) sb.append("  ");
      sb.append(_nid).append(" ");
      return sb.append("pred=").append(_pred).append("\n");
    }

    public final double pred() { return _pred; }
    public final void pred(double pred) { _pred = pred; }
  }

  // --------------------------------------------------------------------------
  public static abstract class TreeModel extends water.Model {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    @API(help="Expected max trees")                public final int N;
    @API(help="MSE rate as trees are added")       public final double [] errs;
    @API(help="Keys of actual trees built")        public final Key [/*N*/][/*nclass*/] treeKeys; // Always filled, but 2-binary classifiers can contain null for 2nd class
    @API(help="Maximum tree depth")                public final int max_depth;
    @API(help="Fewest allowed observations in a leaf") public final int min_rows;
    @API(help="Bins in the histograms")            public final int nbins;

    // For classification models, we'll do a Confusion Matrix right in the
    // model (for now - really should be separate).
    @API(help="Testing key for cm and errs")                                          public final Key testKey;
    // Confusion matrix per each generated tree or null
    @API(help="Confusion Matrix computed on training dataset, cm[actual][predicted]") public final ConfusionMatrix cms[/*CM-per-tree*/];
    @API(help="Confusion matrix domain.")                                             public final String[]        cmDomain;
    @API(help="Variable importance for individual input variables.")                  public final VarImp          varimp;
    @API(help="Tree statistics")                                                      public final TreeStats       treeStats;
    @API(help="AUC for validation dataset")                                           public final AUC             validAUC;

    private transient CompressedTree[/*N*/][/*nclasses OR 1 for regression*/] _treeBitsCache;

    public TreeModel(Key key, Key dataKey, Key testKey, String names[], String domains[][], String[] cmDomain, int ntrees, int max_depth, int min_rows, int nbins) {
      super(key,dataKey,names,domains);
      this.N = ntrees; this.errs = new double[0];
      this.testKey = testKey; this.cms = new ConfusionMatrix[0];
      this.max_depth = max_depth; this.min_rows = min_rows; this.nbins = nbins;
      treeKeys = new Key[0][];
      treeStats = null;
      this.cmDomain = cmDomain!=null ? cmDomain : new String[0];;
      this.varimp = null;
      this.validAUC = null;
    }
    // Simple copy ctor, null value of parameter means copy from prior-model
    private TreeModel(TreeModel prior, Key[][] treeKeys, double[] errs, ConfusionMatrix[] cms, TreeStats tstats, VarImp varimp, AUC validAUC) {
      super(prior._key,prior._dataKey,prior._names,prior._domains);
      this.N = prior.N; this.testKey = prior.testKey;
      this.max_depth = prior.max_depth;
      this.min_rows  = prior.min_rows;
      this.nbins     = prior.nbins;
      this.cmDomain  = prior.cmDomain;

      if (treeKeys != null) this.treeKeys  = treeKeys; else this.treeKeys  = prior.treeKeys;
      if (errs     != null) this.errs      = errs;     else this.errs      = prior.errs;
      if (cms      != null) this.cms       = cms;      else this.cms       = prior.cms;
      if (tstats   != null) this.treeStats = tstats;   else this.treeStats = prior.treeStats;
      if (varimp   != null) this.varimp    = varimp;   else this.varimp    = prior.varimp;
      if (validAUC != null) this.validAUC  = validAUC; else this.validAUC  = prior.validAUC;
    }

    public TreeModel(TreeModel prior, DTree[] tree, double err, ConfusionMatrix cm, TreeStats tstats) {
      this(prior, append(prior.treeKeys, tree), Utils.append(prior.errs, err), Utils.append(prior.cms, cm), tstats, null, null);
    }
    public TreeModel(TreeModel prior, DTree[] tree, TreeStats tstats) {
      this(prior, append(prior.treeKeys, tree), null, null, tstats, null, null);
    }

    public TreeModel(TreeModel prior, double err, ConfusionMatrix cm, VarImp varimp, water.api.AUC validAUC) {
      this(prior, null, Utils.append(prior.errs, err), Utils.append(prior.cms, cm), null, varimp, validAUC);
    }

    private static final Key[][] append(Key[][] prior, DTree[] tree ) {
      if (tree==null) return prior;
      prior = Arrays.copyOf(prior, prior.length+1);
      Key ts[] = prior[prior.length-1] = new Key[tree.length];
      for( int c=0; c<tree.length; c++ )
        if( tree[c] != null ) {
            ts[c] = tree[c].save();
        }
      return prior;
    }

    /** Number of trees in current model. */
    public int ntrees() { return treeKeys.length; }
    // Most recent ConfusionMatrix
    @Override public ConfusionMatrix cm() {
      ConfusionMatrix[] cms = this.cms; // Avoid racey update; read it once
      if(cms != null && cms.length > 0){
        int n = cms.length-1;
        while(n > 0 && cms[n] == null)--n;
        return cms[n] == null?null:cms[n];
      } else return null;
    }

    @Override public VarImp varimp() { return varimp; }
    @Override public double mse() {
      if(errs != null && errs.length > 0){
        int n = errs.length-1;
        while(n > 0 && Double.isNaN(errs[n]))--n;
        return errs[n];
      } else return Double.NaN;
    }
    @Override protected float[] score0(double data[], float preds[]) {
      Arrays.fill(preds,0);
      for( int tidx=0; tidx<treeKeys.length; tidx++ )
        score0(data, preds, tidx);
      return preds;
    }

    /** Returns i-th tree represented by an array of k-trees. */
    public final synchronized CompressedTree[] ctree(int tidx) {
      if (_treeBitsCache!=null && _treeBitsCache[tidx]!=null) return _treeBitsCache[tidx];
      if (_treeBitsCache==null) _treeBitsCache = new CompressedTree[ntrees()][];
      Key[] k = treeKeys[tidx];
      CompressedTree[] ctree = new CompressedTree[nclasses()];
      for (int i = 0; i < nclasses(); i++) // binary classifiers can contains null for second tree
        if (k[i]!=null) ctree[i] = UKV.get(k[i]);
      _treeBitsCache[tidx] = ctree;
      return ctree;
    }
    // Score per line per tree
    public void score0(double data[], float preds[], int treeIdx) {
      CompressedTree ts[] = ctree(treeIdx);
      for( int c=0; c<ts.length; c++ )
        if( ts[c] != null )
          preds[ts.length==1?0:c+1] += ts[c].score(data);
    }

    /** Delete model trees */
    public void delete_trees() {
      Futures fs = new Futures();
      delete_trees(fs);
      fs.blockForPending();
    }
    public Futures delete_trees(Futures fs) {
      for (int tid = 0; tid < treeKeys.length; tid++) /* over all trees */
          for (int cid = 0; cid < treeKeys[tid].length; cid++) /* over all classes */
            // 2-binary classifiers can contain null for the second
            if (treeKeys[tid][cid]!=null) DKV.remove(treeKeys[tid][cid], fs);
      return fs;
    }

    // If model is deleted then all trees has to be delete as well
    @Override public Futures delete_impl(Futures fs) {
      delete_trees(fs);
      super.delete_impl(fs);
      return fs;
    }

    public void generateHTML(String title, StringBuilder sb) {
      DocGen.HTML.title(sb,title);
      DocGen.HTML.paragraph(sb,"Model Key: "+_key);
      DocGen.HTML.paragraph(sb,"Max depth: "+max_depth+", Min rows: "+min_rows+", Nbins:"+nbins);
      generateModelDescription(sb);
      DocGen.HTML.paragraph(sb,water.api.Predict.link(_key,"Predict!"));
      String[] domain = cmDomain; // Domain of response col

      // Generate a display using the last scored Model.  Not all models are
      // scored immediately (since scoring can be a big part of model building).
      ConfusionMatrix cm = null;
      int last = cms.length-1;
      while( last > 0 && cms[last]==null ) last--;
      cm = 0 <= last && last < cms.length ? cms[last] : null;

      // Display the CM
      if( cm != null && domain != null ) {
        // Top row of CM
        assert cm._arr.length==domain.length;
        DocGen.HTML.section(sb,"Confusion Matrix");
        if( testKey == null ) {
          sb.append("<div class=\"alert\">Reported on ").append(title.contains("DRF") ? "out-of-bag" : "training").append(" data</div>");
        } else {
          RString rs = new RString("<div class=\"alert\">Reported on <a href='Inspect2.html?src_key=%$key'>%key</a></div>");
          rs.replace("key", testKey);
          DocGen.HTML.paragraph(sb,rs.toString());
        }

        DocGen.HTML.arrayHead(sb);
        sb.append("<tr class='warning' style='min-width:60px'>");
        sb.append("<th style='min-width:60px'>Actual / Predicted</th>"); // Row header
        for( int i=0; i<cm._arr.length; i++ )
          sb.append("<th style='min-width:60px'>").append(domain[i]).append("</th>");
        sb.append("<th style='min-width:60px'>Error</th>");
        sb.append("</tr>");

        // Main CM Body
        long tsum=0, terr=0;               // Total observations & errors
        for( int i=0; i<cm._arr.length; i++ ) { // Actual loop
          sb.append("<tr style='min-width:60px'>");
          sb.append("<th style='min-width:60px'>").append(domain[i]).append("</th>");// Row header
          long sum=0, err=0;                     // Per-class observations & errors
          for( int j=0; j<cm._arr[i].length; j++ ) { // Predicted loop
            sb.append(i==j ? "<td style='background-color:LightGreen; min-width:60px;'>":"<td style='min-width:60px'>");
            sb.append(cm._arr[i][j]).append("</td>");
            sum += cm._arr[i][j];              // Per-class observations
            if( i != j ) err += cm._arr[i][j]; // and errors
          }
          sb.append(String.format("<th style='min-width:60px'>%5.3f = %d / %d</th>", (double)err/sum, err, sum));
          tsum += sum;  terr += err; // Bump totals
        }
        sb.append("</tr>");

        // Last row of CM
        sb.append("<tr style='min-width:60px'>");
        sb.append("<th style='min-width:60px'>Totals</th>");// Row header
        for( int j=0; j<cm._arr.length; j++ ) { // Predicted loop
          long sum=0;
          for( int i=0; i<cm._arr.length; i++ ) sum += cm._arr[i][j];
          sb.append("<td style='min-width:60px'>").append(sum).append("</td>");
        }
        sb.append(String.format("<th style='min-width:60px'>%5.3f = %d / %d</th>", (double)terr/tsum, terr, tsum));
        sb.append("</tr>");
        DocGen.HTML.arrayTail(sb);
      }

      if( errs != null ) {
        DocGen.HTML.section(sb,"Mean Squared Error by Tree");
        DocGen.HTML.arrayHead(sb);
        sb.append("<tr style='min-width:60px'><th>Trees</th>");
        last = isClassifier() ? last : errs.length-1; // for regressor reports all errors
        for( int i=last; i>=0; i-- )
          sb.append("<td style='min-width:60px'>").append(i).append("</td>");
        sb.append("</tr>");
        sb.append("<tr><th class='warning'>MSE</th>");
        for( int i=last; i>=0; i-- )
          sb.append(!Double.isNaN(errs[i]) ? String.format("<td style='min-width:60px'>%5.3f</td>",errs[i]) : "<td style='min-width:60px'>---</td>");
        sb.append("</tr>");
        DocGen.HTML.arrayTail(sb);
      }
      // Show AUC for binary classifiers
      if (validAUC != null) generateHTMLAUC(sb);

      // Show tree stats
      if (treeStats != null) generateHTMLTreeStats(sb);

      // Show variable importance
      if (varimp != null) generateHTMLVarImp(sb);
    }

    static final String NA = "---";
    protected void generateHTMLTreeStats(StringBuilder sb) {
      DocGen.HTML.section(sb,"Tree stats");
      DocGen.HTML.arrayHead(sb);
      sb.append("<tr><th>&nbsp;</th>").append("<th>Min</th><th>Mean</th><th>Max</th></tr>");

      boolean valid = treeStats.isValid();
      sb.append("<tr><th>Depth</th>")
            .append("<td>").append(valid ? treeStats.minDepth  : NA).append("</td>")
            .append("<td>").append(valid ? treeStats.meanDepth : NA).append("</td>")
            .append("<td>").append(valid ? treeStats.maxDepth  : NA).append("</td></tr>");
      sb.append("<th>Leaves</th>")
            .append("<td>").append(valid ? treeStats.minLeaves  : NA).append("</td>")
            .append("<td>").append(valid ? treeStats.meanLeaves : NA).append("</td>")
            .append("<td>").append(valid ? treeStats.maxLeaves  : NA).append("</td></tr>");
      DocGen.HTML.arrayTail(sb);
    }

    protected void generateHTMLVarImp(StringBuilder sb) {
      if (varimp!=null) {
        // Set up variable names for importance
        varimp.setVariables(Arrays.copyOf(_names, _names.length-1));
        varimp.toHTML(sb);
      }
    }

    protected void generateHTMLAUC(StringBuilder sb) {
      validAUC.toHTML(sb);
    }

    public static class TreeStats extends Iced {
      static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
      static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
      @API(help="Minimal tree depth.") public int minDepth = Integer.MAX_VALUE;
      @API(help="Maximum tree depth.") public int maxDepth = Integer.MIN_VALUE;
      @API(help="Average tree depth.") public float meanDepth;
      @API(help="Minimal num. of leaves.") public int minLeaves = Integer.MAX_VALUE;
      @API(help="Maximum num. of leaves.") public int maxLeaves = Integer.MIN_VALUE;
      @API(help="Average num. of leaves.") public float meanLeaves;

      transient long sumDepth  = 0;
      transient long sumLeaves = 0;
      transient int  numTrees = 0;
      public boolean isValid() { return minDepth <= maxDepth; }
      public void updateBy(DTree[] ktrees) {
        if (ktrees==null) return;
        for (int i=0; i<ktrees.length; i++) {
          DTree tree = ktrees[i];
          if( tree == null ) continue;
          if (minDepth > tree.depth) minDepth = tree.depth;
          if (maxDepth < tree.depth) maxDepth = tree.depth;
          if (minLeaves > tree.leaves) minLeaves = tree.leaves;
          if (maxLeaves < tree.leaves) maxLeaves = tree.leaves;
          sumDepth += tree.depth;
          sumLeaves += tree.leaves;
          numTrees++;
          meanDepth = ((float)sumDepth / numTrees);
          meanLeaves = ((float)sumLeaves / numTrees);
        }
      }
    }

    // --------------------------------------------------------------------------
    // Highly compressed tree encoding:
    //    tree: 1B nodeType, 2B colId, 4B splitVal, left-tree-size, left, right
    //    nodeType: (from lsb):
    //        2 bits ( 1,2) skip-tree-size-size,
    //        1 bit  ( 4) operator flag (0 --> <, 1 --> == ),
    //        1 bit  ( 8) left leaf flag,
    //        1 bit  (16) left leaf type flag, (unused)
    //        1 bit  (32) right leaf flag,
    //        1 bit  (64) right leaf type flag (unused)
    //    left, right: tree | prediction
    //    prediction: 4 bytes of float
    public static class CompressedTree extends Iced {
      final byte [] _bits;
      final int _nclass;
      final long _seed;
      public CompressedTree( byte [] bits, int nclass, long seed ) { _bits = bits; _nclass = nclass; _seed = seed; }
      public float score( final double row[] ) {
        AutoBuffer ab = new AutoBuffer(_bits);
        while(true) {
          int nodeType = ab.get1();
          int colId = ab.get2();
          if( colId == 65535 ) return scoreLeaf(ab);
          float splitVal = ab.get4f();

          boolean equal = ((nodeType&4)==4);
          // Compute the amount to skip.
          int lmask =  nodeType & 0x1B;
          int rmask = (nodeType & 0x60) >> 2;
          int skip = 0;
          switch(lmask) {
          case 0:  skip = ab.get1();  break;
          case 1:  skip = ab.get2();  break;
          case 2:  skip = ab.get3();  break;
          case 3:  skip = ab.get4();  break;
          case 8:  skip = _nclass < 256?1:2;  break; // Small leaf
          case 24: skip = 4;          break; // skip the prediction
          default: assert false:"illegal lmask value " + lmask+" at "+ab.position()+" in bitpile "+Arrays.toString(_bits);
          }

          // WARNING: Generated code has to be consistent with this code:
          //   - Double.NaN <  3.7f => return false => BUT left branch has to be selected (i.e., ab.position())
          //   - Double.NaN != 3.7f => return true  => left branch has to be select selected (i.e., ab.position())
          if( !Double.isNaN(row[colId]) ) { // NaNs always go to bin 0
            if( ( equal && ((float)row[colId]) == splitVal) ||
                (!equal && ((float)row[colId]) >= splitVal) ) {
              ab.position(ab.position()+skip); // Skip to the right subtree
              lmask = rmask;                   // And set the leaf bits into common place
            }
          } /* else Double.isNaN() is true => use left branch */
          if( (lmask&8)==8 ) return scoreLeaf(ab);
        }
      }

      private float scoreLeaf( AutoBuffer ab ) { return ab.get4f(); }

      public Random rngForChunk( int cidx ) {
        Random rand = createRNG(_seed);
        // Argh - needs polishment
        for( int i=0; i<cidx; i++ ) rand.nextLong();
        long seed = rand.nextLong();
        return createRNG(seed);
      }
    }

    /** Abstract visitor class for serialized trees.*/
    public static abstract class TreeVisitor<T extends Exception> {
      // Override these methods to get walker behavior.
      protected void pre ( int col, float fcmp, boolean equal ) throws T { }
      protected void mid ( int col, float fcmp, boolean equal ) throws T { }
      protected void post( int col, float fcmp, boolean equal ) throws T { }
      protected void leaf( float pred )                         throws T { }
      long  result( ) { return 0; } // Override to return simple results

      protected final TreeModel _tm;
      protected final CompressedTree _ct;
      private final AutoBuffer _ts;
      protected int _depth; // actual depth
      protected int _nodes; // number of visited nodes
      public TreeVisitor( TreeModel tm, CompressedTree ct ) {
        _tm = tm;
        _ts = new AutoBuffer((_ct=ct)._bits);
      }

      // Call either the single-class leaf or the full-prediction leaf
      private final void leaf2( int mask ) throws T {
        assert (mask==0 || ( (mask&8)== 8 && (mask&16)==16) ) : "Unknown mask: " + mask;   // Is a leaf or a special leaf on the top of tree
        leaf(_ts.get4f());
      }

      public final void visit() throws T {
        int nodeType = _ts.get1();
        int col = _ts.get2();
        if( col==65535 ) { leaf2(nodeType); return; }
        float fcmp = _ts.get4f();
        boolean equal = ((nodeType&4)==4);
        // Compute the amount to skip.
        int lmask =  nodeType & 0x1B;
        int rmask = (nodeType & 0x60) >> 2;
        int skip = 0;
        switch(lmask) {
        case 0:  skip = _ts.get1();  break;
        case 1:  skip = _ts.get2();  break;
        case 2:  skip = _ts.get3();  break;
        case 3:  skip = _ts.get4();  break;
        case 8:  skip = _ct._nclass < 256?1:2;  break; // Small leaf
        case 24: skip = _ct._nclass*4;  break; // skip the p-distribution
        default: assert false:"illegal lmask value " + lmask;
        }
        pre (col,fcmp,equal);   // Pre-walk
        _depth++;
        if( (lmask & 0x8)==8 ) leaf2(lmask);  else  visit();
        mid (col,fcmp,equal);   // Mid-walk
        if( (rmask & 0x8)==8 ) leaf2(rmask);  else  visit();
        _depth--;
        post(col,fcmp,equal);
        _nodes++;
      }
    }

    StringBuilder toString(final String res, CompressedTree ct, final StringBuilder sb ) {
      new TreeVisitor<RuntimeException>(this,ct) {
        @Override protected void pre( int col, float fcmp, boolean equal ) {
          for( int i=0; i<_depth; i++ ) sb.append("  ");
          sb.append(_names[col]).append(equal?"==":"< ").append(fcmp).append('\n');
        }
        @Override protected void leaf( float pred ) {
          for( int i=0; i<_depth; i++ ) sb.append("  ");
          sb.append(res).append("=").append(pred).append(";\n");
        }
      }.visit();
      return sb;
    }

    // For GBM: learn_rate.  For DRF: mtries, sample_rate, seed.
    abstract protected void generateModelDescription(StringBuilder sb);

    public void toJavaHtml( StringBuilder sb ) {
      if( treeStats == null ) return; // No trees yet
      sb.append("<br /><br /><div class=\"pull-right\"><a href=\"#\" onclick=\'$(\"#javaModel\").toggleClass(\"hide\");\'" +
                "class=\'btn btn-inverse btn-mini\'>Java Model</a></div><br /><div class=\"hide\" id=\"javaModel\">"       +
                "<pre style=\"overflow-y:scroll;\"><code class=\"language-java\">");

      if( ntrees() * treeStats.meanLeaves > 5000 ) {
        String modelName = JCodeGen.toJavaId(_key.toString());
        sb.append("/* Java code is too large to display, download it directly.\n");
        sb.append("   To obtain the code please invoke in your terminal:\n");
        sb.append("     curl http:/").append(H2O.SELF.toString()).append("/h2o-model.jar > h2o-model.jar\n");
        sb.append("     curl http:/").append(H2O.SELF.toString()).append("/2/").append(this.getClass().getSimpleName()).append("View.java?_modelKey=").append(_key).append(" > ").append(modelName).append(".java\n");
        sb.append("     javac -cp h2o-model.jar -J-Xmx2g -J-XX:MaxPermSize=128m ").append(modelName).append(".java\n");
        sb.append("     java -cp h2o-model.jar:. -Xmx2g -XX:MaxPermSize=256m -XX:ReservedCodeCacheSize=256m ").append(modelName).append('\n');
        sb.append("*/");
      } else
        DocGen.HTML.escape(sb,toJava());
      sb.append("</code></pre></div>");
    }

    @Override protected SB toJavaInit(SB sb, SB fileContextSB) {
      sb = super.toJavaInit(sb, fileContextSB);

      String modelName = JCodeGen.toJavaId(_key.toString());

      sb.ii(1);
      // Generate main method
      sb.i().p("/**").nl();
      sb.i().p(" * Sample program harness providing an example of how to call predict().").nl();
      sb.i().p(" */").nl();
      sb.i().p("public static void main(String[] args) throws Exception {").nl();
      sb.i(1).p("int iters = args.length > 0 ? Integer.valueOf(args[0]) : DEFAULT_ITERATIONS;").nl();
      sb.i(1).p(modelName).p(" model = new ").p(modelName).p("();").nl();
      sb.i(1).p("model.bench(iters, DataSample.DATA, new float[NCLASSES+1], NTREES);").nl();
      sb.i().p("}").nl();
      sb.di(1);
      sb.p(TO_JAVA_BENCH_FUNC);

      JCodeGen.toStaticVar(sb, "NTREES", ntrees(), "Number of trees in this model.");
      JCodeGen.toStaticVar(sb, "NTREES_INTERNAL", ntrees()*nclasses(), "Number of internal trees in this model (= NTREES*NCLASSES).");
      JCodeGen.toStaticVar(sb, "DEFAULT_ITERATIONS", 10000, "Default number of iterations.");
      // Generate a data in separated class since we do not want to influence size of constant pool of model class
      if( _dataKey != null ) {
        Value dataval = DKV.get(_dataKey);
        water.fvec.Frame frdata = ValueArray.asFrame(dataval);
        water.fvec.Frame frsub = frdata.subframe(_names);
        JCodeGen.toClass(fileContextSB, "// Sample of data used by benchmark\nclass DataSample", "DATA", frsub, 10, "Sample test data.");
      }
      return sb;
    }
    // Convert Tree model to Java
    @Override protected void toJavaPredictBody( final SB bodySb, final SB classCtxSb, final SB fileCtxSb) {
      // AD-HOC maximal number of trees in forest - in fact constant pool size for Forest class (all UTF String + references to static classes).
      // TODO: in future this parameter can be a parameter for generator, as well as maxIters
      final int maxfsize = 4000;
      int fidx = 0; // forest index
      int treesInForest = 0;
      SB forest = new SB();
      // divide trees into small forests per 100 trees
      /* DEBUG line */ bodySb.i().p("// System.err.println(\"Row (gencode.predict): \" + java.util.Arrays.toString(data));").nl();
      bodySb.i().p("java.util.Arrays.fill(preds,0f);").nl();
      for( int c=0; c<nclasses(); c++ ) {
        toJavaForestBegin(bodySb, forest, c, fidx++, maxfsize);
        for( int i=0; i < treeKeys.length; i++ ) {
          CompressedTree cts[] = ctree(i);
          if( cts[c] == null ) continue;
          forest.i().p("if (iters-- > 0) pred").p(" +=").p(" Tree_").p(i).p("_class_").p(c).p(".predict(data);").nl();
          // append representation of tree predictor
          toJavaTreePredictFct(fileCtxSb, cts[c], i, c);
          if (++treesInForest == maxfsize) {
            toJavaForestEnd(bodySb, forest, c, fidx);
            toJavaForestBegin(bodySb, forest, c, fidx++, maxfsize);
            treesInForest = 0;
          }
        }
        toJavaForestEnd(bodySb, forest, c, fidx);
        treesInForest = 0;
        fidx = 0;
      }
      fileCtxSb.p(forest);
      toJavaUnifyPreds(bodySb);
      toJavaFillPreds0(bodySb);
    }

    /** Generates code which unify preds[1,...NCLASSES] */
    protected void toJavaUnifyPreds(SB bodySb) {
    }
    /** Fill preds[0] based on already filled and unified preds[1,..NCLASSES]. */
    protected void toJavaFillPreds0(SB bodySb) {
      // Pick max index as a prediction
      if (isClassifier()) bodySb.i().p("preds[0] = water.util.ModelUtils.getPrediction(preds,data);").nl();
      else bodySb.i().p("preds[0] = preds[1];").nl();
    }

    /* Numeric type used in generated code to hold predicted value between the calls. */
    static final String PRED_TYPE = "float";

    private void toJavaForestBegin(SB predictBody, SB forest, int c, int fidx, int maxTreesInForest) {
      predictBody.i().p("// Call forest predicting class ").p(c).nl();
      predictBody.i().p("preds[").p(c+1).p("] +=").p(" Forest_").p(fidx).p("_class_").p(c).p(".predict(data, maxIters - "+fidx*maxTreesInForest+");").nl();
      forest.i().p("// Forest representing a subset of trees scoring class ").p(c).nl();
      forest.i().p("class Forest_").p(fidx).p("_class_").p(c).p(" {").nl().ii(1);
      forest.i().p("public static ").p(PRED_TYPE).p(" predict(double[] data, int maxIters) {").nl().ii(1);
      forest.i().p(PRED_TYPE).p(" pred  = 0;").nl();
      forest.i().p("int   iters = maxIters;").nl();
    }
    private void toJavaForestEnd(SB predictBody, SB forest, int c, int fidx) {
      forest.i().p("return pred;").nl();
      forest.i().p("}").di(1).nl(); // end of function
      forest.i().p("}").di(1).nl(); // end of forest classs
    }

    // Produce prediction code for one tree
    protected void toJavaTreePredictFct(final SB sb, final CompressedTree cts, int treeIdx, int classIdx) {
      // generate top-leve class definition
      sb.nl();
      sb.i().p("// Tree predictor for ").p(treeIdx).p("-tree and ").p(classIdx).p("-class").nl();
      sb.i().p("class Tree_").p(treeIdx).p("_class_").p(classIdx).p(" {").nl().ii(1);
      new TreeJCodeGen(this,cts, sb).generate();
      sb.i().p("}").nl(); // close the class
    }

    @Override protected String toJavaDefaultMaxIters() { return String.valueOf(this.N);  }
  }

  // Build a compressed-tree struct
  public TreeModel.CompressedTree compress() {
    int sz = root().size();
    if( root() instanceof LeafNode ) sz += 3; // Oops - tree-stump
    AutoBuffer ab = new AutoBuffer(sz);
    if( root() instanceof LeafNode ) // Oops - tree-stump
      ab.put1(0).put2((char)65535); // Flag it special so the decompress doesn't look for top-level decision
    root().compress(ab);      // Compress whole tree
    assert ab.position() == sz;
    return new TreeModel.CompressedTree(ab.buf(),_nclass,_seed);
  }
  /** Save this tree into DKV store under default random Key. */
  public Key save() { return save(defaultTreeKey()); }
  /** Save this tree into DKV store under the given Key. */
  public Key save(Key k) {
    CompressedTree ts = compress();
    UKV.put(k, ts);
    return k;
  }

  private Key defaultTreeKey() {
    return Key.make("__Tree_"+Key.rand());
  }

  private static final SB TO_JAVA_BENCH_FUNC = new SB().
      nl().
      p("  /**").nl().
      p("   * Run a predict() benchmark with the generated model and some synthetic test data.").nl().
      p("   *").nl().
      p("   * @param iters number of iterations to run; each iteration predicts on every sample (i.e. row) in the test data").nl().
      p("   * @param data test data to predict on").nl().
      p("   * @param preds output predictions").nl().
      p("   * @param ntrees number of trees").nl().
      p("   */").nl().
      p("  public void bench(int iters, double[][] data, float[] preds, int ntrees) {").nl().
      p("    System.out.println(\"Iterations: \" + iters);").nl().
      p("    System.out.println(\"Data rows : \" + data.length);").nl().
      p("    System.out.println(\"Trees     : \" + ntrees + \"x\" + (preds.length-1));").nl().
      nl().
      p("    long startMillis;").nl().
      p("    long endMillis;").nl().
      p("    long deltaMillis;").nl().
      p("    double deltaSeconds;").nl().
      p("    double samplesPredicted;").nl().
      p("    double samplesPredictedPerSecond;").nl().
      p("    System.out.println(\"Starting timing phase of \"+iters+\" iterations...\");").nl().
      nl().
      p("    startMillis = System.currentTimeMillis();").nl().
      p("    for (int i=0; i<iters; i++) {").nl().
      p("      // Uncomment the nanoTime logic for per-iteration prediction times.").nl().
      p("      // long startTime = System.nanoTime();").nl().
      nl().
      p("      for (double[] row : data) {").nl().
      p("        predict(row, preds);").nl().
      p("        // System.out.println(java.util.Arrays.toString(preds) + \" : \" + (DOMAINS[DOMAINS.length-1]!=null?(DOMAINS[DOMAINS.length-1][(int)preds[0]]+\"~\"+DOMAINS[DOMAINS.length-1][(int)row[row.length-1]]):(preds[0] + \" ~ \" + row[row.length-1])) );").nl().
      p("      }").nl().
      nl().
      p("      // long ttime = System.nanoTime()-startTime;").nl().
      p("      // System.out.println(i+\". iteration took \" + (ttime) + \"ns: scoring time per row: \" + ttime/data.length +\"ns, scoring time per row and tree: \" + ttime/data.length/ntrees + \"ns\");").nl().
      nl().
      p("      if ((i % 1000) == 0) {").nl().
      p("        System.out.println(\"finished \"+i+\" iterations (of \"+iters+\")...\");").nl().
      p("      }").nl().
      p("    }").nl().
      p("    endMillis = System.currentTimeMillis();").nl().
      nl().
      p("    deltaMillis = endMillis - startMillis;").nl().
      p("    deltaSeconds = (double)deltaMillis / 1000.0;").nl().
      p("    samplesPredicted = data.length * iters;").nl().
      p("    samplesPredictedPerSecond = samplesPredicted / deltaSeconds;").nl().
      p("    System.out.println(\"finished in \"+deltaSeconds+\" seconds.\");").nl().
      p("    System.out.println(\"samplesPredicted: \" + samplesPredicted);").nl().
      p("    System.out.println(\"samplesPredictedPerSecond: \" + samplesPredictedPerSecond);").nl().
      p("  }").nl().
  nl();

  static class TreeJCodeGen extends TreeVisitor<RuntimeException> {
    public static final int MAX_NODES = (1 << 12) / 4; // limit of decision nodes
    final byte  _bits[]  = new byte [100];
    final float _fs  []  = new float[100];
    final SB    _sbs []  = new SB   [100];
    final int   _nodesCnt[] = new int  [100];
    SB _sb;
    SB _csb;

    int _subtrees = 0;

    public TreeJCodeGen(TreeModel tm, CompressedTree ct, SB sb) {
      super(tm, ct);
      _sb = sb;
      _csb = new SB();
    }

    // code preambule
    protected void preamble(SB sb, int subtree) throws RuntimeException {
      String subt = subtree>0?String.valueOf(subtree):"";
      sb.i().p("static final ").p(TreeModel.PRED_TYPE).p(" predict").p(subt).p("(double[] data) {").nl().ii(1); // predict method for one tree
      sb.i().p(TreeModel.PRED_TYPE).p(" pred = ");
    }

    // close the code
    protected void closure(SB sb) throws RuntimeException {
      sb.p(";").nl();
      sb.i(1).p("return pred;").nl().di(1);
      sb.i().p("}").nl().di(1);
    }

    @Override protected void pre( int col, float fcmp, boolean equal ) {
      if( _depth > 0 ) {
        int b = _bits[_depth-1];
        assert b > 0 : Arrays.toString(_bits)+"\n"+_sb.toString();
        if( b==1         ) _bits[_depth-1]=3;
        if( b==1 || b==2 ) _sb.p('\n').i(_depth).p("?");
        if( b==2         ) _sb.p(' ').pj(_fs[_depth-1]); // Dump the leaf containing float value
        if( b==2 || b==3 ) _sb.p('\n').i(_depth).p(":");
      }
      if (_nodes>MAX_NODES) {
        _sb.p("predict").p(_subtrees).p("(data)");
        _nodesCnt[_depth] = _nodes;
        _sbs[_depth] = _sb;
        _sb = new SB();
        _nodes = 0;
        preamble(_sb, _subtrees);
        _subtrees++;
      }
      // All NAs are going always to the left
      _sb.p(" (Double.isNaN(data[").p(col).p("]) || (float) data[").p(col).p(" /* ").p(_tm._names[col]).p(" */").p("] ").p(equal?"!= ":"< ").pj(fcmp); // then left and then right (left is !=)
      assert _bits[_depth]==0;
      _bits[_depth]=1;
    }
    @Override protected void leaf( float pred  ) {
      assert _depth==0 || _bits[_depth-1] > 0 : Arrays.toString(_bits); // it can be degenerated tree
      if( _depth==0) { // it is de-generated tree
        _sb.pj(pred);
      } else if( _bits[_depth-1] == 1 ) { // No prior leaf; just memoize this leaf
        _bits[_depth-1]=2; _fs[_depth-1]=pred;
      } else {          // Else==2 (prior leaf) or 3 (prior tree)
        if( _bits[_depth-1] == 2 ) _sb.p(" ? ").pj(_fs[_depth-1]).p(" ");
        else                       _sb.p('\n').i(_depth);
        _sb.p(": ").pj(pred);
      }
    }
    @Override protected void post( int col, float fcmp, boolean equal ) {
      _sb.p(')');
      _bits[_depth]=0;
      if (_sbs[_depth]!=null) {
        closure(_sb);
        _csb.p(_sb);
        _sb = _sbs[_depth];
        _nodes = _nodesCnt[_depth];
        _sbs[_depth] = null;
      }
    }
    public void generate() {
      preamble(_sb, _subtrees++);
      visit();
      closure(_sb);
      _sb.p(_csb);
    }
  }
}
