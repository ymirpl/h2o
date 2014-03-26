package water.fvec;

import water.*;
import water.nbhm.NonBlockingHashMapLong;
import water.util.Utils;

import java.util.Arrays;
import java.util.UUID;

import static water.util.Utils.seq;

/**
 * A single distributed vector column.
 * <p>
 * A distributed vector has a count of elements, an element-to-chunk mapping, a
 * Java type (mostly determines rounding on store and display), and functions
 * to directly load elements without further indirections.  The data is
 * compressed, or backed by disk or both.  *Writing* to elements may throw if the
 * backing data is read-only (file backed).
 * <p>
 * <pre>
 *  Vec Key format is: Key. VEC - byte, 0 - byte,   0    - int, normal Key bytes.
 * DVec Key format is: Key.DVEC - byte, 0 - byte, chunk# - int, normal Key bytes.
 * </pre>
 *
 * The main API is at, set, and isNA:<br>
 *<pre>
 *   double  at  ( long row );  // Returns the value expressed as a double.  NaN if missing.
 *   long    at8 ( long row );  // Returns the value expressed as a long.  Throws if missing.
 *   boolean isNA( long row );  // True if the value is missing.
 *   set( long row, double d ); // Stores a double; NaN will be treated as missing.
 *   set( long row, long l );   // Stores a long; throws if l exceeds what fits in a double & any floats are ever set.
 *   setNA( long row );         // Sets the value as missing.
 * </pre>
 *
 * Note this dangerous scenario: loading a missing value as a double, and
 * setting it as a long: <pre>
 *   set(row,(long)at(row)); // Danger!
 *</pre>
 * The cast from a Double.NaN to a long produces a zero!  This code will
 * replace a missing value with a zero.
 *
 * @author Cliff Click
 */
public class Vec extends Iced {
  /** Log-2 of Chunk size. */
  public static final int LOG_CHK = ValueArray.LOG_CHK; // Same as VA to help conversions
  /** Chunk size.  Bigger increases batch sizes, lowers overhead costs, lower
   * increases fine-grained parallelism. */
  static final int CHUNK_SZ = 1 << LOG_CHK;

  /** Key mapping a Value which holds this Vec.  */
  final public Key _key;        // Top-level key
  /** Element-start per chunk.  Always zero for chunk 0.  One more entry than
   *  chunks, so the last entry is the total number of rows.  This field is
   *  dead/ignored in subclasses that are guaranteed to have fixed-sized chunks
   *  such as file-backed Vecs. */
  final public long _espc[];

  /** Enum/factor/categorical names. */
  public String [] _domain;
  /** Time parse, index into Utils.TIME_PARSE, or -1 for not-a-time */
  public byte _time;
  /** RollupStats: min/max/mean of this Vec lazily computed.  */
  private double _min, _max, _mean, _sigma;
  long _size;
  boolean _isInt;
  /** The count of missing elements.... or -2 if we have active writers and no
   *  rollup info can be computed (because the vector is being rapidly
   *  modified!), or -1 if rollups have not been computed since the last
   *  modification.   */
  volatile long _naCnt=-1;

  /** Maximal size of enum domain */
  public static final int MAX_ENUM_SIZE = 10000;

  /** Main default constructor; requires the caller understand Chunk layout
   *  already, along with count of missing elements.  */
  public Vec( Key key, long espc[] ) {
    assert key._kb[0]==Key.VEC;
    _key = key;
    _espc = espc;
    _time = -1;                 // not-a-time
  }

  protected Vec( Key key, Vec v ) { _key = key; _espc = v._espc; _time = -1; assert group()==v.group(); }

  // A 1-element Vec
  public Vec( Key key, double d ) {
    _key = key;
    _espc = new long[]{0,1};
    Futures fs = new Futures();
    DKV.put(chunkKey(0),new C0DChunk(d,1),fs);
    DKV.put(_key,this,fs);
    fs.blockForPending();
  }

  /** Make a new vector with the same size and data layout as the old one, and
   *  initialized to zero. */
  public Vec makeZero() { return makeCon(0); }
  /** Make a new vector with the same size and data layout as the old one, and
   *  initialized to a constant. */
  public Vec makeCon( final long l ) {
    Futures fs = new Futures();
    if( _espc == null ) throw H2O.unimpl(); // need to make espc for e.g. NFSFileVecs!
    int nchunks = nChunks();
    Vec v0 = new Vec(group().addVecs(1)[0],_espc);
    long row=0;                 // Start row
    for( int i=0; i<nchunks; i++ ) {
      long nrow = chunk2StartElem(i+1); // Next row
      DKV.put(v0.chunkKey(i),new C0LChunk(l,(int)(nrow-row)),fs);
      row = nrow;
    }
    DKV.put(v0._key,v0,fs);
    fs.blockForPending();
    return v0;
  }
  public Vec makeCon( final double d ) {
    Futures fs = new Futures();
    if( _espc == null ) throw H2O.unimpl(); // need to make espc for e.g. NFSFileVecs!
    if( (long)d==d ) return makeCon((long)d);
    int nchunks = nChunks();
    Vec v0 = new Vec(group().addVecs(1)[0],_espc);
    long row=0;                 // Start row
    for( int i=0; i<nchunks; i++ ) {
      long nrow = chunk2StartElem(i+1); // Next row
      DKV.put(v0.chunkKey(i),new C0DChunk(d,(int)(nrow-row)),fs);
      row = nrow;
    }
    DKV.put(v0._key,v0,fs);
    fs.blockForPending();
    return v0;
  }
  public static Vec makeSeq( int len ) {
    Futures fs = new Futures();
    AppendableVec av = new AppendableVec(VectorGroup.VG_LEN1.addVec());
    NewChunk nc = new NewChunk(av,0);
    for (int r = 0; r < len; r++) nc.addNum(r+1);
    nc.close(0,fs);
    Vec v = av.close(fs);
    fs.blockForPending();
    return v;
  }

  /** Create a vector transforming values according given domain map.
   * @see Vec#makeTransf(int[], int[], String[])
   */
  public Vec makeTransf(final int[][] map, String[] domain) { return makeTransf(map[0], map[1], domain); }
  public Vec makeTransf(final int[][] map) { return makeTransf(map[0], map[1], null); }
  Vec makeTransf(final int[] values, final int[] indexes) { return makeTransf(values, indexes, null); }
  /**
   * TODO
   * @param values
   * @param indexes
   * @param domain
   * @return
   */
  Vec makeTransf(final int[] values, final int[] indexes, final String[] domain) {
    if( _espc == null ) throw H2O.unimpl();
    Vec v0 = new TransfVec(values, indexes, domain, this._key, group().addVecs(1)[0],_espc);
    UKV.put(v0._key,v0);
    return v0;
  }
  /**
   * TODO
   * @return
   * @see Vec#makeTransf(int[], int[], String[])
   */
  Vec makeIdentityTransf() {
    assert _domain != null : "Cannot make an identity transformation of non-enum vector!";
    return makeTransf(seq(0, _domain.length), null, _domain);
  }
  /**
   * TODO
   * @param values
   * @param domain
   * @return
   * @see Vec#makeTransf(int[], int[], String[])
   */
  Vec makeSimpleTransf(long[] values, String[] domain) {
    int is[] = new int[values.length];
    for( int i=0; i<values.length; i++ ) is[i] = (int)values[i];
    return makeTransf(is, null, domain);
  }
  /** This Vec does not have dependent hidden Vec it uses.
   *
   * @return dependent hidden vector or <code>null</code>
   */
  public Vec masterVec() { return null; }

  /**
   * Adapt given vector <code>v</code> to this vector.
   * I.e., unify domains, compute transformation, and call makeTransf().
   *
   * This vector is a leader - it determines a domain (i.e., {@link #domain()}) and mapping between values stored in vector
   * and domain values.
   * The vector <code>v</code> can contain different domain (subset, superset), hence the values stored in the vector
   * has to be transformed to the values determined by this vector. The resulting vector domain is the
   * same as this vector domain.
   *
   * Always returns a new vector and user's responsibility is delete the vector.
   *
   * @param v vector which should be adapter in according this vector.
   * @param exact should vector match exactly (recommended value is true).
   * @return a new vector which implements transformation of original values.
   */
  /*// Not used any more in code ??
  public Vec adaptTo(Vec v, boolean exact) {
    assert isInt() : "This vector has to be int/enum vector!";
    int[] domain = null;
    // Compute domain of this vector
    // - if vector is enum, use domain directly
    // - if vector is int, then vector numeric domain is collected and transformed to string domain
    // and then adapted
    String[] sdomain =
        (_domain == null)
        ? Utils.toStringMap(domain = new CollectDomain(this).doAll(this).domain()) // it is number-column
        : domain(); // it is enum
    // Compute transformation - domain map - each value in an array is one value from vector domain, its index
    // represents an index into string domain representation.
    int[] domMap = Model.getDomainMapping(v._domain, sdomain, exact);
    if (domain!=null) {
      // do a mapping from INT -> ENUM -> this vector ENUM
      domMap = Utils.compose(Utils.mapping(domain), domMap);
    }
    return this.makeTransf(domMap, sdomain);
  }*/

  /** Number of elements in the vector.  Overridden by subclasses that compute
   *  length in an alternative way, such as file-backed Vecs. */
  public long length() { return _espc[_espc.length-1]; }

  /** Number of chunks.  Overridden by subclasses that compute chunks in an
   *  alternative way, such as file-backed Vecs. */
  public int nChunks() { return _espc.length-1; }

  /** Is the column a factor/categorical/enum?  Note: all "isEnum()" columns
   *  are are also "isInt()" but not vice-versa. */
  public final boolean isEnum(){return _domain != null;}

  /** Whether or not this column parsed as a time, and if so what pattern was used. */
  public final boolean isTime(){ return _time>=0; }
  public final int timeMode(){ return _time; }
  public final String timeParse(){ return ParseTime.TIME_PARSE[_time]; }

  /** Map the integer value for a enum/factor/categorical to it's String.
   *  Error if it is not an ENUM.  */
  public String domain(long i) { return _domain[(int)i]; }

  /** Return an array of domains.  This is eagerly manifested for enum or
   *  categorical columns.  Returns null for non-Enum/factor columns. */
  public String[] domain() { return _domain; }

  /** Returns cardinality for enum domain or -1 for other types. */
  public int cardinality() { return isEnum() ? _domain.length : -1; }

  /** Transform this vector to enum.
   *  If the vector is integer vector then its domain is collected and transformed to
   *  corresponding strings.
   *  If the vector is enum an identity transformation vector is returned.
   *  Transformation is done by a {@link TransfVec} which provides a mapping between values.
   *
   *  @return always returns a new vector and the caller is responsible for vector deletion!
   */
  public Vec toEnum() {
    if( isEnum() ) return this.makeIdentityTransf(); // Make an identity transformation of this vector
    if( !isInt() ) throw new IllegalArgumentException("Enum conversion only works on integer columns");
    long[] domain;
    String[] sdomain = Utils.toStringMap(domain = new CollectDomain(this).doAll(this).domain());
    if( domain.length > MAX_ENUM_SIZE ) throw new IllegalArgumentException("Column domain is too large to be represented as an enum: " + domain.length + " > " + MAX_ENUM_SIZE);
    return this.makeSimpleTransf(domain, sdomain);
  }

  /** Default read/write behavior for Vecs.  File-backed Vecs are read-only. */
  protected boolean readable() { return true ; }
  /** Default read/write behavior for Vecs.  AppendableVecs are write-only. */
  protected boolean writable() { return true; }

  /** Return column min - lazily computed as needed. */
  public double min()  { return rollupStats()._min; }
  /** Return column max - lazily computed as needed. */
  public double max()  { return rollupStats()._max; }
  /** Return column mean - lazily computed as needed. */
  public double mean() { return rollupStats()._mean; }
  /** Return column standard deviation - lazily computed as needed. */
  public double sigma(){ return rollupStats()._sigma; }
  /** Return column missing-element-count - lazily computed as needed. */
  public long  naCnt() { return rollupStats()._naCnt; }
  /** Is all integers? */
  public boolean isInt(){return rollupStats()._isInt; }
  /** Size of compressed vector data. */
  public long byteSize(){return rollupStats()._size; }

  Vec setRollupStats( RollupStats rs ) {
    _min  = rs._min; _max = rs._max; _mean = rs._mean;
    _sigma = Math.sqrt(rs._sigma / (rs._rows - 1));
    _size =rs._size;
    _isInt= rs._isInt;
    if( rs._rows == 0 )         // All rows missing?  Then no rollups
      _min = _max = _mean = _sigma = Double.NaN;
    _naCnt= rs._naCnt;          // Volatile write last to announce all stats ready
    return this;
  }

  /** Compute the roll-up stats as-needed, and copy into the Vec object */
  public Vec rollupStats() { return rollupStats(null); }
  // Allow a bunch of rollups to run in parallel.  If Futures is passed in, run
  // the rollup in the background.  *Always* returns "this".
  public Vec rollupStats(Futures fs) {
    Vec vthis = DKV.get(_key).get();
    if( vthis._naCnt==-2 ) throw new IllegalArgumentException("Cannot ask for roll-up stats while the vector is being actively written.");
    if( vthis._naCnt>= 0 ) {    // KV store has a better answer
      if( vthis == this ) return this;
      _min  = vthis._min;   _max   = vthis._max;
      _mean = vthis._mean;  _sigma = vthis._sigma;
      _size = vthis._size;  _isInt = vthis._isInt;
      _naCnt= vthis._naCnt;  // Volatile write last to announce all stats ready
    } else {                 // KV store reports we need to recompute
      RollupStats rs = new RollupStats().dfork(this);
      if(fs != null) fs.add(rs); else setRollupStats(rs.getResult());
    }
    return this;
  }

  /** A private class to compute the rollup stats */
  private static class RollupStats extends MRTask2<RollupStats> {
    double _min=Double.MAX_VALUE, _max=-Double.MAX_VALUE, _mean, _sigma;
    long _rows, _naCnt, _size;
    boolean _isInt=true;

    @Override public void postGlobal(){
      final RollupStats rs = this;
      _fr.vecs()[0].setRollupStats(rs);
      // Now do this remotely also
      new TAtomic<Vec>() {
        @Override public Vec atomic(Vec v) {
          if( v!=null && v._naCnt == -1 ) v.setRollupStats(rs);  return v;
        }
      }.fork(_fr._keys[0]);
    }

    @Override public void map( Chunk c ) {
      _size = c.byteSize();
      for( int i=0; i<c._len; i++ ) {
        double d = c.at0(i);
        if( Double.isNaN(d) ) _naCnt++;
        else {
          if( d < _min ) _min = d;
          if( d > _max ) _max = d;
          _mean += d;
          _rows++;
          if( _isInt && ((long)d) != d ) _isInt = false;
        }
      }
      _mean = _mean / _rows;
      for( int i=0; i<c._len; i++ ) {
        if( !c.isNA0(i) ) {
          double d = c.at0(i);
          _sigma += (d - _mean) * (d - _mean);
        }
      }
    }
    @Override public void reduce( RollupStats rs ) {
      _min = Math.min(_min,rs._min);
      _max = Math.max(_max,rs._max);
      _naCnt += rs._naCnt;
      double delta = _mean - rs._mean;
      if (_rows == 0) { _mean = rs._mean;  _sigma = rs._sigma; }
      else if (rs._rows > 0) {
        _mean = (_mean*_rows + rs._mean*rs._rows)/(_rows + rs._rows);
        _sigma = _sigma + rs._sigma + delta*delta * _rows*rs._rows / (_rows+rs._rows);
      }
      _rows += rs._rows;
      _size += rs._size;
      _isInt &= rs._isInt;
    }
    // Just toooo common to report always.  Drowning in multi-megabyte log file writes.
    @Override public boolean logVerbose() { return false; }
  }

  /** Writing into this Vector from *some* chunk.  Immediately clear all caches
   *  (_min, _max, _mean, etc).  Can be called repeatedly from one or all
   *  chunks.  Per-chunk row-counts will not be changing, just row contents and
   *  caches of row contents. */
  void preWriting( ) {
    if( _naCnt == -2 ) return; // Already set
    _naCnt = -2;
    if( !writable() ) throw new IllegalArgumentException("Vector not writable");
    // Set remotely lazily.  This will trigger a cloud-wide invalidate of the
    // existing Vec, and eventually we'll have to load a fresh copy of the Vec
    // with active writing turned on, and caching disabled.
    new TAtomic<Vec>() {
      @Override public Vec atomic(Vec v) { if( v!=null ) v._naCnt=-2; return v; }
    }.invoke(_key);
  }

  /** Stop writing into this Vec.  Rollup stats will again (lazily) be computed. */
  public void postWrite() {
    Vec vthis = DKV.get(_key).get();
    if( vthis._naCnt==-2 ) {
      _naCnt = vthis._naCnt=-1;
      new TAtomic<Vec>() {
        @Override public Vec atomic(Vec v) { if( v!=null && v._naCnt==-2 ) v._naCnt=-1; return v; }
      }.invoke(_key);
    }
  }

  /** Convert a row# to a chunk#.  For constant-sized chunks this is a little
   *  shift-and-add math.  For variable-sized chunks this is a binary search,
   *  with a sane API (JDK has an insane API).  Overridden by subclasses that
   *  compute chunks in an alternative way, such as file-backed Vecs. */
  int elem2ChunkIdx( long i ) {
    assert 0 <= i && i < length() : "0 <= "+i+" < "+length();
    int lo=0, hi = nChunks();
    while( lo < hi-1 ) {
      int mid = (hi+lo)>>>1;
      if( i < _espc[mid] ) hi = mid;
      else                 lo = mid;
    }
    while( _espc[lo+1] == i ) lo++;
    return lo;
  }

  /** Convert a chunk-index into a starting row #.  For constant-sized chunks
   *  this is a little shift-and-add math.  For variable-sized chunks this is a
   *  table lookup. */
  public long chunk2StartElem( int cidx ) { return _espc[cidx]; }

  /** Number of rows in chunk. Does not fetch chunk content. */
  public int chunkLen( int cidx ) { return (int) (_espc[cidx + 1] - _espc[cidx]); }

  /** Get a Vec Key from Chunk Key, without loading the Chunk */
  static public Key getVecKey( Key key ) {
    assert key._kb[0]==Key.DVEC;
    byte [] bits = key._kb.clone();
    bits[0] = Key.VEC;
    UDP.set4(bits,6,-1); // chunk#
    return Key.make(bits);
  }

  /** Get a Chunk Key from a chunk-index.  Basically the index-to-key map. */
  public Key chunkKey(int cidx ) {
    byte [] bits = _key._kb.clone();
    bits[0] = Key.DVEC;
    UDP.set4(bits,6,cidx); // chunk#
    return Key.make(bits);
  }
  /** Get a Chunk's Value by index.  Basically the index-to-key map,
   *  plus the {@code DKV.get()}.  Warning: this pulls the data locally;
   *  using this call on every Chunk index on the same node will
   *  probably trigger an OOM!  */
  public Value chunkIdx( int cidx ) {
    Value val = DKV.get(chunkKey(cidx));
    assert checkMissing(cidx,val);
    return val;
  }

  protected boolean checkMissing(int cidx, Value val) {
    if( val != null ) return true;
    System.out.println("Error: Missing chunk "+cidx+" for "+_key);
    return false;
  }


  /** Make a new random Key that fits the requirements for a Vec key. */
  static public Key newKey(){return newKey(Key.make());}

  public static final int KEY_PREFIX_LEN = 4+4+1+1;
  /** Make a new Key that fits the requirements for a Vec key, based on the
   *  passed-in key.  Used to make Vecs that back over e.g. disk files. */
  static Key newKey(Key k) {
    byte [] kb = k._kb;
    byte [] bits = MemoryManager.malloc1(kb.length+KEY_PREFIX_LEN);
    bits[0] = Key.VEC;
    bits[1] = -1;         // Not homed
    UDP.set4(bits,2,0);   // new group, so we're the first vector
    UDP.set4(bits,6,-1);  // 0xFFFFFFFF in the chunk# area
    System.arraycopy(kb, 0, bits, 4+4+1+1, kb.length);
    return Key.make(bits);
  }

  /** Make a Vector-group key.  */
  private Key groupKey(){
    byte [] bits = _key._kb.clone();
    bits[0] = Key.VGROUP;
    UDP.set4(bits, 2, -1);
    UDP.set4(bits, 6, -1);
    return Key.make(bits);
  }
  /**
   * Get the group this vector belongs to.
   * In case of a group with only one vector, the object actually does not exist in KV store.
   *
   * @return VectorGroup this vector belongs to.
   */
  public final VectorGroup group() {
    Key gKey = groupKey();
    Value v = DKV.get(gKey);
    if(v != null)return v.get(VectorGroup.class);
    // no group exists so we have to create one
    return new VectorGroup(gKey,1);
  }

  /** The Chunk for a chunk#.  Warning: this loads the data locally!  */
  public Chunk chunkForChunkIdx(int cidx) {
    long start = chunk2StartElem(cidx); // Chunk# to chunk starting element#
    Value dvec = chunkIdx(cidx);        // Chunk# to chunk data
    Chunk c = dvec.get();               // Chunk data to compression wrapper
    long cstart = c._start;             // Read once, since racily filled in
    if( cstart == start ) return c;     // Already filled-in
    assert cstart == -1;       // Was not filled in (everybody racily writes the same start value)
    c._start = start;          // Fields not filled in by unpacking from Value
    c._vec = this;             // Fields not filled in by unpacking from Value
    return c;
  }
  /** The Chunk for a row#.  Warning: this loads the data locally!  */
  public final Chunk chunkForRow(long i) {
    return chunkForChunkIdx(elem2ChunkIdx(i));
  }

  /** Fetch element the slow way, as a long.  Floating point values are
   *  silently rounded to an integer.  Throws if the value is missing. */
  public final long  at8( long i ) { return chunkForRow(i).at8(i); }
  /** Fetch element the slow way, as a double.  Missing values are
   *  returned as Double.NaN instead of throwing. */
  public final double at( long i ) { return chunkForRow(i).at(i); }
  /** Fetch the missing-status the slow way. */
  public final boolean isNA(long row){ return chunkForRow(row).isNA(row); }

  /** Write element the slow way, as a long.  There is no way to write a
   *  missing value with this call.  Under rare circumstances this can throw:
   *  if the long does not fit in a double (value is larger magnitude than
   *  2^52), AND float values are stored in Vector.  In this case, there is no
   *  common compatible data representation. */
  public final long   set( long i, long   l) { return chunkForRow(i).set(i, l); }
  /** Write element the slow way, as a double.  Double.NaN will be treated as
   *  a set of a missing element. */
  public final double set( long i, double d) { return chunkForRow(i).set(i, d); }
  /** Write element the slow way, as a float.  Float.NaN will be treated as
   *  a set of a missing element. */
  public final float  set( long i, float  f) { return chunkForRow(i).set(i, f); }
  /** Set the element as missing the slow way.  */
  public final boolean setNA( long i ) { return chunkForRow(i).setNA(i); }

  /** Pretty print the Vec: [#elems, min/mean/max]{chunks,...} */
  @Override public String toString() {
    String s = "["+length()+(_naCnt<0 ? ", {" : ","+_min+"/"+_mean+"/"+_max+", "+PrettyPrint.bytes(_size)+", {");
    int nc = nChunks();
    for( int i=0; i<nc; i++ ) {
      s += chunkKey(i).home_node()+":"+chunk2StartElem(i)+":";
      // CNC: Bad plan to load remote data during a toString... messes up debug printing
      // Stupidly chunkForChunkIdx loads all data locally
      // s += chunkForChunkIdx(i).getClass().getSimpleName().replaceAll("Chunk","")+", ";
    }
    return s+"}]";
  }

  public void remove( Futures fs ) {
    for( int i=0; i<nChunks(); i++ )
      UKV.remove(chunkKey(i),fs);
  }

  @Override public boolean equals( Object o ) {
    return o instanceof Vec && ((Vec)o)._key.equals(_key);
  }
  @Override public int hashCode() { return _key.hashCode(); }

  /** Always makes a copy of the given vector which shares the same
   * group.
   *
   * The user is responsible for deleting the returned vector.
   *
   * This can be expensive operation since it can force copy of data
   * among nodes.
   *
   * @param vec vector which is intended to be copied
   * @return a copy of vec which shared the same {@link VectorGroup} with this vector
   */
  public Vec align(final Vec vec) {
    assert ! this.group().equals(vec.group()) : "Vector align expects a vector from different vector group";
    assert this._size == vec._size : "Trying to align vectors with different length!";
    Vec avec = makeZero(); // aligned vector
    new MRTask2() {
      @Override public void map(Chunk c0) {
        long srow = c0._start;
        for (int r = 0; r < c0._len; r++) c0.set0(r, vec.at(srow + r));
      }
    }.doAll(avec);
    avec._domain = _domain;
    return avec;
  }

  /**
   * Class representing the group of vectors.
   *
   * Vectors from the same group have same distribution of chunks among nodes.
   * Each vector is member of exactly one group.  Default group of one vector
   * is created for each vector.  Group of each vector can be retrieved by
   * calling group() method;
   *
   * The expected mode of operation is that user wants to add new vectors
   * matching the source.  E.g. parse creates several vectors (one for each
   * column) which are all colocated and are colocated with the original
   * bytevector.
   *
   * To do this, user should first ask for the set of keys for the new vectors
   * by calling addVecs method on the target group.
   *
   * Vectors in the group will have the same keys except for the prefix which
   * specifies index of the vector inside the group.  The only information the
   * group object carries is it's own key and the number of vectors it
   * contains(deleted vectors still count).
   *
   * Because vectors(and chunks) share the same key-pattern with the group,
   * default group with only one vector does not have to be actually created,
   * it is implicit.
   *
   * @author tomasnykodym
   *
   */
  public static class VectorGroup extends Iced {
    // The common shared vector group for length==1 vectors
    public static VectorGroup VG_LEN1 = new VectorGroup();
    final int _len;
    final Key _key;
    private VectorGroup(Key key, int len){_key = key;_len = len;}
    public VectorGroup() {
      byte[] bits = new byte[26];
      bits[0] = Key.VGROUP;
      bits[1] = -1;
      UDP.set4(bits, 2, -1);
      UDP.set4(bits, 6, -1);
      UUID uu = UUID.randomUUID();
      UDP.set8(bits,10,uu.getLeastSignificantBits());
      UDP.set8(bits,18,uu. getMostSignificantBits());
      _key = Key.make(bits);
      _len = 0;
    }
    public Key vecKey(int vecId){
      byte [] bits = _key._kb.clone();
      bits[0] = Key.VEC;
      UDP.set4(bits,2,vecId);//
      return Key.make(bits);
    }
    /**
     * Task to atomically add vectors into existing group.
     * @author tomasnykodym
     */
    private static class AddVecs2GroupTsk extends TAtomic<VectorGroup>{
      final Key _key;
      int _n;          // INPUT: Keys to allocate; OUTPUT: start of run of keys
      private AddVecs2GroupTsk(Key key, int n){_key = key; _n = n;}
      @Override public VectorGroup atomic(VectorGroup old) {
        int n = _n;             // how many
        // If the old group is missing, assume it is the default group-of-self
        // (having 1 ID already allocated for self), not a new group with
        // zero prior vectors.
        _n = old==null ? 1 : old._len; // start of allocated key run
        return new VectorGroup(_key, n+_n);
      }
    }
    // reserve range of keys and return index of first new available key
    public int reserveKeys(final int n){
      AddVecs2GroupTsk tsk = new AddVecs2GroupTsk(_key, n);
      tsk.invoke(_key);
      return tsk._n;
    }
    /**
     * Gets the next n keys of this group.
     * Performs atomic update of the group object to assure we get unique keys.
     * The group size will be updated by adding n.
     *
     * @param n number of keys to make
     * @return arrays of unique keys belonging to this group.
     */
    public Key [] addVecs(final int n){
      AddVecs2GroupTsk tsk = new AddVecs2GroupTsk(_key, n);
      tsk.invoke(_key);
      Key [] res = new Key[n];
      for(int i = 0; i < n; ++i)
        res[i] = vecKey(i + tsk._n);
      return res;
    }
    /**
     * Shortcut for addVecs(1).
     * @see #addVecs(int)
     */
    public Key addVec() {
      return addVecs(1)[0];
    }

    @Override public String toString() {
      return "VecGrp "+_key.toString()+", next free="+_len;
    }

    @Override public boolean equals( Object o ) {
      return o instanceof VectorGroup && ((VectorGroup)o)._key.equals(_key);
    }
    @Override public int hashCode() {
      return _key.hashCode();
    }
  }

  /** Collect numeric domain of given vector */
  public static class CollectDomain extends MRTask2<CollectDomain> {
    transient NonBlockingHashMapLong<Object> _uniques;
    @Override protected void setupLocal() { _uniques = new NonBlockingHashMapLong(); }
    public CollectDomain(Vec v) { }
    @Override public void map(Chunk ys) {
      for( int row=0; row<ys._len; row++ )
        if( !ys.isNA0(row) )
          _uniques.put(ys.at80(row),"");
    }

    @Override public void reduce(CollectDomain mrt) {
      if( _uniques == mrt._uniques ) return;
      _uniques.putAll(mrt._uniques);
    }

    @Override public AutoBuffer write( AutoBuffer ab ) {
      super.write(ab);
      return ab.putA8(_uniques==null ? null : _uniques.keySetLong());
    }

    @Override public CollectDomain read( AutoBuffer ab ) {
      super.read(ab);
      assert _uniques == null || _uniques.size()==0;
      long ls[] = ab.getA8();
      _uniques = new NonBlockingHashMapLong();
      if( ls != null ) for( long l : ls ) _uniques.put(l,"");
      return this;
    }
    @Override public void copyOver(DTask that) {
      super.copyOver(that);
      _uniques = ((CollectDomain)that)._uniques;
    }

    /** Returns exact numeric domain of given vector computed by this task.
     * The domain is always sorted. Hence:
     *    domain()[0] - minimal domain value
     *    domain()[domain().length-1] - maximal domain value
     */
    public long[] domain() {
      long[] dom = _uniques.keySetLong();
      Arrays.sort(dom);
      return dom;
    }
  }
}

