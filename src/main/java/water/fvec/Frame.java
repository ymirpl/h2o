package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.exec.Flow;
import water.fvec.Vec.VectorGroup;

/**
 * A collection of named Vecs.  Essentially an R-like data-frame.  Multiple
 * Frames can reference the same Vecs.  A Frame is a lightweight object, it is
 * meant to be cheaply created and discarded for data munging purposes.
 * E.g. to exclude a Vec from a computation on a Frame, create a new Frame that
 * references all the Vecs but this one.
 */
public class Frame extends Lockable<Frame> {
  public String[] _names;
  Key[] _keys;          // Keys for the vectors
  private transient Vec[] _vecs;// The Vectors (transient to avoid network traffic)
  private transient Vec _col0;  // First readable vec; fast access to the VectorGroup's Chunk layout

  public Frame( Frame fr ) { this(fr._key,fr._names.clone(), fr.vecs().clone()); _col0 = null; }
  public Frame( Vec... vecs ){ this(null,vecs);}
  public Frame( String[] names, Vec[] vecs ) { this(null,names,vecs); }
  public Frame( Key key, String[] names, Vec[] vecs ) {
    super(key);
    // assert names==null || names.length == vecs.length : "Number of columns does not match to number of cols' names.";
    _names=names;
    _vecs=vecs;
    _keys = new Key[vecs.length];
    for( int i=0; i<vecs.length; i++ ) {
      Key k = _keys[i] = vecs[i]._key;
      if( DKV.get(k)==null )    // If not already in KV, put it there
        DKV.put(k,vecs[i]);
    }
    assert checkCompatible();
  }
  public Vec vec(String name){
    Vec [] vecs = vecs();
    for(int i = 0; i < _names.length; ++i)
      if(_names[i].equals(name))return vecs[i];
    return null;
  }
  public Frame subframe(String [] names){
    Vec [] vecs = new Vec[names.length];
    vecs();                     // Preload the vecs
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    for(int i = 0; i < _names.length; ++i)map.put(_names[i], i);
    for(int i = 0; i < names.length; ++i)
      if(map.containsKey(names[i])) vecs[i] = _vecs[map.get(names[i])];
      else throw new IllegalArgumentException("Missing column called "+names[i]);
    return new Frame(names,vecs);
  }
  public final Vec[] vecs() {
    Vec[] tvecs = _vecs; // read the content
    if( tvecs != null ) return tvecs; // compare and return directly
    // Load all Vec headers; load them all in parallel by spawning F/J tasks.
    final Vec [] vecs = new Vec[_keys.length];
    Futures fs = new Futures();
    for( int i=0; i<_keys.length; i++ ) {
      final int ii = i;
      final Key k = _keys[i];
      H2OCountedCompleter t = new H2OCountedCompleter() {
          // We need higher priority here as there is a danger of deadlock in
          // case of many calls from MRTask2 at once (e.g. frame with many
          // vectors invokes rollup tasks for all vectors in parallel).  Should
          // probably be done in CPS style in the future
          @Override public byte priority(){return H2O.MIN_HI_PRIORITY;}
          @Override public void compute2() {
            Value v = DKV.get(k);
            if( v==null ) System.err.println("Missing vector during Frame fetch: "+k);
            vecs[ii] = v.get();
            tryComplete();
          }
        };
      H2O.submitTask(t);
      fs.add(t);
    }
    fs.blockForPending();
    return _vecs = vecs;
  }
  // Force a cache-flush & reload, assuming vec mappings were altered remotely
  public final Vec[] reloadVecs() { _vecs=null; return vecs(); }

  /** Finds the first column with a matching name.  */
  public int find( String name ) {
    if (_names!=null)
      for( int i=0; i<_names.length; i++ )
        if( name.equals(_names[i]) )
          return i;
    return -1;
  }

  public int find( Vec vec ) {
    for( int i=0; i<_vecs.length; i++ )
      if( vec.equals(_vecs[i]) )
        return i;
    return -1;
  }

 /** Appends a named column, keeping the last Vec as the response */
  public Frame add( String name, Vec vec ) {
    assert _vecs.length == 0 || anyVec().group().equals(vec.group());
    if( find(name) != -1 ) throw new IllegalArgumentException("Duplicate name '"+name+"' in Frame");
    final int len = _names != null ? _names.length : 0;
    _names = _names != null ? Arrays.copyOf(_names,len+1) : new String[len+1];
    _vecs  = _names != null ? Arrays.copyOf(_vecs ,len+1) : new Vec   [len+1];
    _keys  = _names != null ? Arrays.copyOf(_keys ,len+1) : new Key   [len+1];
    _names[len] = name;
    _vecs [len] = vec ;
    _keys [len] = vec._key;
    return this;
  }

  /** Appends an entire Frame */
  public Frame add( Frame fr, String names[] ) {
    assert _vecs.length==0 || anyVec().group().equals(fr.anyVec().group());
    for( String name : names )
      if( find(name) != -1 ) throw new IllegalArgumentException("Duplicate name '"+name+"' in Frame");
    final int len0= _names!=null ? _names.length : 0;
    final int len1=  names!=null ?  names.length : 0;
    final int len = len0+len1;
    // Note: _names==null <=> _vecs==null <=> _keys==null
    _names = _names != null ? Arrays.copyOf(_names,len) : new String[len];
    _vecs  = _vecs  != null ? Arrays.copyOf(_vecs ,len) : new Vec   [len];
    _keys  = _keys  != null ? Arrays.copyOf(_keys ,len) : new Key   [len];
    System.arraycopy(    names,0,_names,len0,len1);
    System.arraycopy(fr._vecs ,0,_vecs ,len0,len1);
    System.arraycopy(fr._keys ,0,_keys ,len0,len1);
    return this;
  }
  public Frame add( Frame fr, boolean rename ) {
    if( !rename ) return add(fr,fr._names);
    String names[] = new String[fr._names.length];
    for( int i=0; i<names.length; i++ ) {
      String name = fr._names[i];
      int cnt=0;
      while( find(name) != -1 )
        name = fr._names[i]+(cnt++);
      names[i] = name;
    }
    return add(fr,names);
  }

  /** Removes the first column with a matching name.  */
  public Vec remove( String name ) { return remove(find(name)); }

  /** Removes a numbered column. */
  public Vec [] remove( int [] idxs ) {
    for(int i :idxs)if(i < 0 || i > _vecs.length)
      throw new ArrayIndexOutOfBoundsException();
    Arrays.sort(idxs);
    Vec [] res = new Vec[idxs.length];
    Vec [] rem = new Vec[_vecs.length-idxs.length];
    String [] names = new String[rem.length];
    Key    [] keys  = new Key   [rem.length];
    int j = 0;
    int k = 0;
    int l = 0;
    for(int i = 0; i < _vecs.length; ++i)
      if(j < idxs.length && i == idxs[j]){
        ++j;
        res[k++] = _vecs[i];
      } else {
        rem  [l] = _vecs [i];
        names[l] = _names[i];
        keys [l] = _keys [i];
        ++l;
      }
    _vecs = rem;
    _names = names;
    _keys = keys;
    assert l == rem.length && k == idxs.length;
    return res;
  }
  /** Removes a numbered column. */
  public Vec remove( int idx ) {
    int len = _names.length;
    if( idx < 0 || idx >= len ) return null;
    Vec v = vecs()[idx];
    System.arraycopy(_names,idx+1,_names,idx,len-idx-1);
    System.arraycopy(_vecs ,idx+1,_vecs ,idx,len-idx-1);
    System.arraycopy(_keys ,idx+1,_keys ,idx,len-idx-1);
    _names = Arrays.copyOf(_names,len-1);
    _vecs  = Arrays.copyOf(_vecs ,len-1);
    _keys  = Arrays.copyOf(_keys ,len-1);
    if( v == _col0 ) _col0 = null;
    return v;
  }

  /**
   * Remove given interval of columns from frame. Motivated by R intervals.
   * @param startIdx - start index of column (inclusive)
   * @param endIdx - end index of column (exclusive)
   * @return an array of remove columns
   */
  public Vec[] remove(int startIdx, int endIdx) {
    int len = _names.length;
    int nlen = len - (endIdx-startIdx);
    String[] names = new String[nlen];
    Key[] keys = new Key[nlen];
    Vec[] vecs = new Vec[nlen];
    if (startIdx > 0) {
      System.arraycopy(_names, 0, names, 0, startIdx);
      System.arraycopy(_vecs,  0, vecs,  0, startIdx);
      System.arraycopy(_keys,  0, keys,  0, startIdx);
    }
    nlen -= startIdx;
    if (endIdx < _names.length+1) {
      System.arraycopy(_names, endIdx, names, startIdx, nlen);
      System.arraycopy(_vecs,  endIdx, vecs,  startIdx, nlen);
      System.arraycopy(_keys,  endIdx, keys,  startIdx, nlen);
    }

    Vec[] vec = Arrays.copyOfRange(vecs(),startIdx,endIdx);
    _names = names;
    _vecs = vec;
    _keys = keys;
    _col0 = null;
    return vec;
  }

  public Vec replace(int col, Vec nv) {
    assert col < _names.length;
    Vec rv = vecs()[col];
    assert rv.group().equals(nv.group());
    _vecs[col] = nv;
    _keys[col] = nv._key;
    if( DKV.get(nv._key)==null )    // If not already in KV, put it there
      DKV.put(nv._key, nv);
    return rv;
  }

  public Frame extractFrame(int startIdx, int endIdx) {
    Frame f = subframe(startIdx, endIdx);
    remove(startIdx, endIdx);
    return f;
  }

  /** Create a subframe from given interval of columns.
   *
   * @param startIdx index of first column (inclusive)
   * @param endIdx index of the last column (exclusive)
   * @return a new frame containing specified interval of columns
   */
  public Frame subframe(int startIdx, int endIdx) {
    Frame result = new Frame(Arrays.copyOfRange(_names,startIdx,endIdx),Arrays.copyOfRange(vecs(),startIdx,endIdx));
    return result;
  }

  public final String[] names() { return _names; }
  public int  numCols() { return vecs().length; }
  public long numRows() { return anyVec()==null ? 0 : anyVec().length(); }

  // Number of columns when categoricals expanded.
  // Note: One level is dropped in each categorical col.
  public int numExpCols() {
    int ncols = 0;
    for(int i = 0; i < vecs().length; i++)
      ncols += vecs()[i].domain() == null ? 1 : (vecs()[i].domain().length - 1);
    return ncols;
  }

  /** All the domains for enum columns; null for non-enum columns.  */
  public String[][] domains() {
    String ds[][] = new String[vecs().length][];
    for( int i=0; i<vecs().length; i++ )
      ds[i] = vecs()[i].domain();
    return ds;
  }

  private String[][] domains(int [] cols){
    Vec [] vecs = vecs();
    String [][] res = new String[cols.length][];
    for(int i = 0; i < cols.length; ++i)
      res[i] = vecs[cols[i]]._domain;
    return res;
  }

  private String [] names(int [] cols){
    if(_names == null)return null;
    String [] res = new String[cols.length];
    for(int i = 0; i < cols.length; ++i)
      res[i] = _names[cols[i]];
    return res;
  }

  public Vec lastVec() {
    final Vec [] vecs = vecs();
    return vecs[vecs.length-1];
  }
  /** Returns the first readable vector. */
  public Vec anyVec() {
    if( _col0 != null ) return _col0;
    for( Vec v : vecs() )
      if( v.readable() )
        return (_col0 = v);
    return null;
  }
  /* Returns the only Vector, or tosses IAE */
  public final Vec theVec(String err) {
    if( _keys.length != 1 ) throw new IllegalArgumentException(err);
    if( _vecs == null ) _vecs = new Vec[]{_col0 = DKV.get(_keys[0]).get() };
    return _vecs[0];
  }

  /** Check that the vectors are all compatible.  All Vecs have their content
   *  sharded using same number of rows per chunk.  */
  public boolean checkCompatible( ) {
    Vec v0 = anyVec();
    if( v0 == null ) return true;
    int nchunks = v0.nChunks();
    for( Vec vec : vecs() ) {
      if( vec instanceof AppendableVec ) continue; // New Vectors are endlessly compatible
      if( vec.nChunks() != nchunks )
        throw new IllegalArgumentException("Vectors different numbers of chunks, "+nchunks+" and "+vec.nChunks());
    }
    // Also check each chunk has same rows
    for( int i=0; i<nchunks; i++ ) {
      long es = v0.chunk2StartElem(i);
      for( Vec vec : vecs() )
        if( !(vec instanceof AppendableVec) && vec.chunk2StartElem(i) != es )
          throw new IllegalArgumentException("Vector chunks different numbers of rows, "+es+" and "+vec.chunk2StartElem(i));
    }
    // For larger Frames, verify that the layout is compatible - else we'll be
    // endlessly cache-missing the data around the cluster, pulling copies
    // local everywhere.
    if( v0.length() > 1e4 ) {
      VectorGroup grp = v0.group();
      for( Vec vec : vecs() )
        assert grp.equals(vec.group()) : "Vector " + vec + " has different vector group!";
    }
    return true;
  }

  public void closeAppendables() {closeAppendables(new Futures()).blockForPending(); }
  // Close all AppendableVec
  public Futures closeAppendables(Futures fs) {
    _col0 = null;               // Reset cache
    int len = vecs().length;
    for( int i=0; i<len; i++ ) {
      Vec v = _vecs[i];
      if( v instanceof AppendableVec )
        DKV.put(_keys[i],_vecs[i] = ((AppendableVec)v).close(fs),fs);
    }
    return fs;
  }

  /** Actually remove/delete all Vecs from memory, not just from the Frame. */
  @Override public Futures delete_impl(Futures fs) {
    for( Key k : _keys ) UKV.remove(k,fs);
    _names = new String[0];
    _vecs = new Vec[0];
    _keys = new Key[0];
    return fs;
  }
  @Override public String errStr() { return "Dataset"; }

  public long byteSize() {
    long sum=0;
    for( int i=0; i<vecs().length; i++ )
      sum += _vecs[i].byteSize();
    return sum;
  }

  @Override public String toString() {
    // Across
    Vec vecs[] = vecs();
    if( vecs.length==0 ) return "{}";
    String s="{"+_names[0];
    long bs=vecs[0].byteSize();
    for( int i=1; i<vecs.length; i++ ) {
      s += ","+_names[i];
      bs+= vecs[i].byteSize();
    }
    s += "}, "+PrettyPrint.bytes(bs)+"\n";
    // Down
    Vec v0 = anyVec();
    if( v0 == null ) return s;
    int nc = v0.nChunks();
    s += "Chunk starts: {";
    for( int i=0; i<nc; i++ ) s += v0.chunkForChunkIdx(i)._start+",";
    s += "}";
    return s;
  }
  public String toStringNames() { return Arrays.toString(_names); }

  // Print a row with headers inlined
  private String toStr( long idx, int col ) {
    return _names[col]+"="+(_vecs[col].isNA(idx) ? "NA" : _vecs[col].at(idx));
  }
  public String toString( long idx ) {
    String s="{"+toStr(idx,0);
    for( int i=1; i<_names.length; i++ )
       s += ","+toStr(idx,i);
    return s+"}";
  }

  // Print fixed-width row & fixed-width headers (more compressed print
  // format).  Returns the column formats.
  public String[] toStringHdr( StringBuilder sb ) {
    String[] fs = new String[numCols()];
    for( int c=0; c<fs.length; c++ ) {
      String n = (c < _names.length) ? _names[c] : ("C"+c);
      if( numRows()==0 ) { sb.append(n).append(' '); continue; }
      int w=0;
      if( _vecs[c].isEnum() ) {
        String ss[] = _vecs[c]._domain;
        for( int i=0; i<ss.length; i++ )
          w = Math.max(w,ss[i].length());
        w = Math.min(w,10);
        fs[c] = "%"+w+"."+w+"s";
      } else {
        Chunk C = _vecs[c].chunkForChunkIdx(0);   // 1st Chunk
        // Possible situation: 1) vec is INT - C is has no floats => OK
        // 2) vec is INT - C has floats => IMPOSSIBLE,
        // 3) vec is FLOAT - C has floats => OK,
        // 4) vec is FLOAT - C has no floats => find the first chunk with floats
        if (!_vecs[c].isInt() &&  !C.hasFloat()) {
          for (int i=1; i<_vecs[c].nChunks(); i++) {
            C=_vecs[c].chunkForChunkIdx(i);
            if (C.hasFloat()) break;
          }
        }
        String f = fs[c] = C.pformat();  // Printable width
        for( int x=0; x<f.length(); x++ )// Get printable width from format
          if( Character.isDigit(f.charAt(x)) ) w = w*10+(f.charAt(x)-'0');
          else if( w>0 ) break;
        if( f.charAt(1)==' ' ) w++; // Leading blank is not in print-width
      }
      int len = sb.length();
      if( n.length() <= w ) {          // Short name, big digits
        sb.append(n);
        for( int i=n.length(); i<w; i++ ) sb.append(' ');
      } else if( w==1 ) {       // First char only
        sb.append(n.charAt(0));
      } else if( w==2 ) {       // First 2 chars only
        sb.append(n.charAt(0)).append(n.charAt(1));
      } else {                  // First char dot lastchars; e.g. Compress "Interval" to "I.val"
        sb.append(n.charAt(0)).append('.');
        for( int i=n.length()-(w-2); i<n.length(); i++ )
          sb.append(n.charAt(i));
      }
      assert len+w==sb.length();
      sb.append(' ');           // Column seperator
    }
    sb.append('\n');
    return fs;
  }
  public StringBuilder toString( StringBuilder sb, String[] fs, long idx ) {
    Vec vecs[] = vecs();
    for( int c=0; c<fs.length; c++ ) {
      Vec vec = vecs[c];
      if( vec.isEnum() ) {
        String s = "----------";
        if( !vec.isNA(idx) ) {
          int x = (int)vec.at8(idx);
          if( x >= 0 && x < vec._domain.length ) s = vec._domain[x];
        }
        sb.append(String.format(fs[c],s));
      } else if( vec.isInt() ) {
        if( vec.isNA(idx) ) {
          Chunk C = vec.chunkForChunkIdx(0);   // 1st Chunk
          int len = C.pformat_len0();  // Printable width
          for( int i=0; i<len; i++ ) sb.append('-');
        } else {
          try {
            sb.append(String.format(fs[c],vec.at8(idx)));
          } catch( IllegalFormatException ife ) {
            System.out.println("Format: "+fs[c]+" col="+c+" not for ints");
            ife.printStackTrace();
          }
        }
      } else {
        sb.append(String.format(fs[c],vec.at (idx)));
        if( vec.isNA(idx) ) sb.append(' ');
      }
      sb.append(' ');           // Column seperator
    }
    sb.append('\n');
    return sb;
  }
  public String toStringAll() {
    StringBuilder sb = new StringBuilder();
    String[] fs = toStringHdr(sb);
    for( int i=0; i<numRows(); i++ )
      toString(sb,fs,i);
    return sb.toString();
  }

  // Return the entire Frame as a CSV stream
  public InputStream toCSV(boolean headers) {
    return new CSVStream(headers);
  }

  private class CSVStream extends InputStream {
    byte[] _line;
    int _position;
    long _row;

    CSVStream(boolean headers) {
      StringBuilder sb = new StringBuilder();
      Vec vs[] = vecs();
      if( headers ) {
        sb.append('"' + _names[0] + '"');
        for(int i = 1; i < vs.length; i++)
          sb.append(',').append('"' + _names[i] + '"');
        sb.append('\n');
      }
      _line = sb.toString().getBytes();
    }

    @Override public int available() throws IOException {
      if(_position == _line.length) {
        if(_row == numRows())
          return 0;
        StringBuilder sb = new StringBuilder();
        Vec vs[] = vecs();
        for( int i = 0; i < vs.length; i++ ) {
          if(i > 0) sb.append(',');
          if(!vs[i].isNA(_row)) {
            if(vs[i].isEnum()) sb.append('"' + vs[i]._domain[(int) vs[i].at8(_row)] + '"');
            else if(vs[i].isInt()) sb.append(vs[i].at8(_row));
            else sb.append(vs[i].at(_row));
          }
        }
        sb.append('\n');
        _line = sb.toString().getBytes();
        _position = 0;
        _row++;
      }
      return _line.length - _position;
    }

    @Override public void close() throws IOException {
      super.close();
      _line = null;
    }

    @Override public int read() throws IOException {
      return available() == 0 ? -1 : _line[_position++];
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      int n = available();
      if(n > 0) {
        n = Math.min(n, len);
        System.arraycopy(_line, _position, b, off, n);
        _position += n;
      }
      return n;
    }
  }


  // --------------------------------------------------------------------------
  // In support of R, a generic Deep Copy & Slice.
  // Semantics are a little odd, to match R's.
  // Each dimension spec can be:
  //   null - all of them
  //   a sorted list of negative numbers (no dups) - all BUT these
  //   an unordered list of positive - just these, allowing dups
  // The numbering is 1-based; zero's are not allowed in the lists, nor are out-of-range.
  final int MAX_EQ2_COLS = 100000;      // FIXME.  Put this in a better spot.
  public Frame deepSlice( Object orows, Object ocols ) {
    // ocols is either a long[] or a Frame-of-1-Vec
    long[] cols = null;
    if( ocols == null ) cols = null;
    else if (ocols instanceof long[]) cols = (long[])ocols;
    else if (ocols instanceof Frame) {
      Frame fr = (Frame) ocols;
      if (fr.numCols() != 1)
        throw new IllegalArgumentException("Columns Frame must have only one column (actually has " + fr.numCols() + " columns)");
      long n = fr.anyVec().length();
      if (n > MAX_EQ2_COLS)
        throw new IllegalArgumentException("Too many requested columns (requested " + n +", max " + MAX_EQ2_COLS + ")");
      cols = new long[(int)n];
      Vec v = fr.anyVec();
      for (long i = 0; i < v.length(); i++)
        cols[(int)i] = v.at8(i);
    } else
      throw new IllegalArgumentException("Columns is specified by an unsupported data type (" + ocols.getClass().getName() + ")");
    
    // Since cols is probably short convert to a positive list.
    int c2[] = null;
    if( cols==null ) {
      c2 = new int[numCols()];
      for( int i=0; i<c2.length; i++ ) c2[i]=i;
    } else if( cols.length==0 ) {
      c2 = new int[0];
    } else if( cols[0] > 0 ) {
      c2 = new int[cols.length];
      for( int i=0; i<cols.length; i++ )
        c2[i] = (int)cols[i]-1; // Convert 1-based cols to zero-based
    } else {
      c2 = new int[numCols()-cols.length];
      int j=0;
      for( int i=0; i<numCols(); i++ ) {
        if( j >= cols.length || i < (-cols[j]-1) ) c2[i-j] = i;
        else j++;
      }
    }
    for( int i=0; i<c2.length; i++ )
      if( c2[i] >= numCols() )
        throw new IllegalArgumentException("Trying to select column "+(c2[i]+1)+" but only "+numCols()+" present.");
    if( c2.length==0 )
      throw new IllegalArgumentException("No columns selected (did you try to select column 0 instead of column 1?)");

    // Do Da Slice
    // orows is either a long[] or a Vec
    if (orows == null)
      return new DeepSlice((long[])orows,c2).doAll(c2.length,this).outputFrame(names(c2),domains(c2));
    else if (orows instanceof long[]) {
      final long CHK_ROWS=1000000;
      long[] rows = (long[])orows;
      if( rows.length==0 || rows[0] < 0 )
        return new DeepSlice(rows,c2).doAll(c2.length, this).outputFrame(names(c2), domains(c2));
      // Vec'ize the index array
      Futures fs = new Futures();
      AppendableVec av = new AppendableVec("rownames");
      int r = 0;
      int c = 0;
      while (r < rows.length) {
        NewChunk nc = new NewChunk(av, c);
        long end = Math.min(r+CHK_ROWS, rows.length);
        for (; r < end; r++) {
          nc.addNum(rows[r]);
        }
        nc.close(c++, fs);
      }
      Vec c0 = av.close(fs);   // c0 is the row index vec
      fs.blockForPending();
      Frame fr2 = new Slice(c2, this).doAll(c2.length,new Frame(new String[]{"rownames"}, new Vec[]{c0}))
              .outputFrame(names(c2), domains(c2));
      UKV.remove(c0._key);      // Remove hidden vector
      return fr2;
    }
    Frame frows = (Frame)orows;
    Vec vrows = frows.anyVec();
    // It's a compatible Vec; use it as boolean selector.
    // Build column names for the result.
    Vec [] vecs = new Vec[c2.length+1];
    String [] names = new String[c2.length+1];
    for(int i = 0; i < c2.length; ++i){
      vecs[i] = _vecs[c2[i]];
      names[i] = _names[c2[i]];
    }
    vecs[c2.length] = vrows;
    names[c2.length] = "predicate";
    return new DeepSelect().doAll(c2.length,new Frame(names,vecs)).outputFrame(names(c2),domains(c2));
  }

  // Slice and return in the form of new chunks.
  private static class Slice extends MRTask2<Slice> {
    final Frame  _base;   // the base frame to slice from
    final int[]  _cols;
    Slice(int[] cols, Frame base) { _cols = cols; _base = base; }
    @Override public void map(Chunk[] ix, NewChunk[] ncs) {
      final Vec[] vecs = new Vec[_cols.length];
      final Vec   anyv = _base.anyVec();
      final long  nrow = anyv.length();
            long  r    = ix[0].at80(0);
      int   last_ci = anyv.elem2ChunkIdx(r<nrow?r:0); // memoize the last chunk index
      long  last_c0 = anyv._espc[last_ci];            // ...         last chunk start
      long  last_c1 = anyv._espc[last_ci + 1];        // ...         last chunk end
      Chunk[] last_cs = new Chunk[vecs.length];       // ...         last chunks
      for (int c = 0; c < _cols.length; c++) {
        vecs[c] = _base.vecs()[_cols[c]];
        last_cs[c] = vecs[c].chunkForChunkIdx(last_ci);
      }
      for (int i = 0; i < ix[0]._len; i++) {
        // select one row
        r = ix[0].at80(i) - 1;   // next row to select
        if (r < 0) continue;
        if (r >= nrow) {
          for (int c = 0; c < vecs.length; c++) ncs[c].addNum(Double.NaN);
        } else {
          if (r < last_c0 || r >= last_c1) {
            last_ci = anyv.elem2ChunkIdx(r);
            last_c0 = anyv._espc[last_ci];
            last_c1 = anyv._espc[last_ci + 1];
            for (int c = 0; c < vecs.length; c++)
              last_cs[c] = vecs[c].chunkForChunkIdx(last_ci);
          }
          for (int c = 0; c < vecs.length; c++)
            ncs[c].addNum(last_cs[c].at(r));
        }
      }
    }
  }

  // Bulk (expensive) copy from 2nd cols into 1st cols.
  // Sliced by the given cols & rows
  private static class DeepSlice extends MRTask2<DeepSlice> {
    final int  _cols[];
    final long _rows[];
    boolean _ex = true;
    DeepSlice( long rows[], int cols[]) { _cols=cols; _rows=rows;}
    @Override public void map( Chunk chks[], NewChunk nchks[] ) {
      long rstart = chks[0]._start;
      int rlen = chks[0]._len;  // Total row count
      int rx = 0;               // Which row to in/ex-clude
      int rlo = 0;              // Lo/Hi for this block of rows
      int rhi = rlen;
      while( true ) {           // Still got rows to include?
        if( _rows != null ) {   // Got a row selector?
          if( rx >= _rows.length ) break; // All done with row selections
          long r = _rows[rx++]-1;// Next row selector
          if( r < 0 ) {          // Row exclusion
            if(rx > 0 && _rows[rx - 1] < _rows[rx]) throw H2O.unimpl();
            long er = Math.abs(r) - 2;
            if ( er < rstart) continue;
            //scoop up all of the rows before the first exclusion
            if (rx == 1 && ( (int)(er + 1 - rstart)) > 0 && _ex) {
              rlo = (int)rstart;
              rhi = (int)(er - rstart);
              _ex = false;
              rx--;
            } else {
              rlo = (int)(er + 1 - rstart);
              //TODO: handle jumbled row indices ( e.g. -c(1,5,3) )
              while(rx < _rows.length && (_rows[rx] + 1 == _rows[rx - 1] && rlo < rlen)) {
                if(rx < _rows.length - 1 && _rows[rx] < _rows[rx + 1]) throw H2O.unimpl();
                rx++; rlo++;    //Exclude consecutive rows
              }
              rhi = rx >= _rows.length ? rlen : (int)Math.abs(_rows[rx] - 1) - 2;
              if(rx < _rows.length - 1 && _rows[rx] < _rows[rx + 1]) throw H2O.unimpl();
            }
          } else {              // Positive row list?
            if( r < rstart ) continue;
            rlo = (int)(r-rstart);
            rhi = rlo+1;        // Stop at the next row
            while( rx < _rows.length && (_rows[rx]-1-rstart)==rhi && rhi < rlen ) {
              rx++; rhi++;      // Grab sequential rows
            }
          }
        }
        // Process this next set of rows
        // For all cols in the new set
          for( int i=0; i<_cols.length; i++ ) {
              Chunk    oc =  chks[_cols[i]];
              NewChunk nc = nchks[      i ];
              if( oc._vec.isInt() ) { // Slice on integer columns
                  for( int j=rlo; j<rhi; j++ )
                      if( oc.isNA0(j) ) nc.addNA();
                      else              nc.addNum(oc.at80(j),0);
              } else {                // Slice on double columns
                  for( int j=rlo; j<rhi; j++ )
                      nc.addNum(oc.at0(j));
              }
          }
        rlo=rhi;
        if( _rows==null ) break;
      }
    }
  }

  private static class DeepSelect extends MRTask2<DeepSelect> {
    @Override public void map( Chunk chks[], NewChunk nchks[] ) {
      Chunk pred = chks[chks.length-1];
      for(int i = 0; i < pred._len; ++i){
        if(pred.at0(i) != 0)
          for(int j = 0; j < chks.length-1; ++j)
            nchks[j].addNum(chks[j].at0(i));
      }
    }
  }

  // ------------------------------------------------------------------------------

  public
  <Y extends Flow.PerRow<Y>>      // Type parameter
  Flow.FlowPerRow<Y>              // Return type of with()
  with                            // The method name
  ( Flow.PerRow<Y> pr )           // Arguments for with()
  {
    return new Flow.FlowPerRow<Y>(pr,new Flow.FlowFrame(this));
  }

  public Flow.FlowFilter with( Flow.Filter fr ) {
    return new Flow.FlowFilter(fr,new Flow.FlowFrame(this));
  }

  public Flow.FlowGroupBy with( Flow.GroupBy fr ) {
    return new Flow.FlowGroupBy(fr,new Flow.FlowFrame(this));
  }
}
