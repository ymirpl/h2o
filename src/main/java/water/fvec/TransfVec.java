package water.fvec;

import java.util.Arrays;

import water.*;
import water.util.Utils;

/**
 * Dummy vector transforming values of given vector according to given domain mapping.
 *
 * <p>The mapping is defined by a simple hash map composed of two arrays.
 * The first array contains values. Index of values is index into the second array {@link #_indexes}
 * which contains final value (i.e., index to domain array).</p>
 *
 * <p>If {@link #_indexes} array is null, then index of found value is used directly.</p>
 *
 * <p>To avoid virtual calls or additional null check for {@link #_indexes} the vector
 * returns two implementation of underlying chunk ({@link TransfChunk} when {@link #_indexes} is not <code>null</code>,
 * and {@link FlatTransfChunk} when {@link #_indexes} is <code>null</code>.</p>
 */
public class TransfVec extends WrappedVec {
  /** List of values from underlying vector which this vector map to a new value. If
   * a value is not included in this array the implementation returns NA. */
  final int[] _values;
  /** The transformed value - i.e. transformed value is: <code>int idx = find(value, _values); return _indexes[idx]; </code> */
  final int[] _indexes;

  public TransfVec(int[][] mapping, Key masterVecKey, Key key, long[] espc) {
    this(mapping, null, masterVecKey, key, espc);
  }
  public TransfVec(int[][] mapping, String[] domain, Key masterVecKey, Key key, long[] espc) {
    this(mapping[0], mapping[1], domain, masterVecKey, key, espc);
  }
  public TransfVec(int[] values, int[] indexes, String[] domain, Key masterVecKey, Key key, long[] espc) {
    super(masterVecKey, key, espc);
    _values  =  values;
    _indexes =  indexes;
    _domain = domain;
  }

  @Override public Chunk chunkForChunkIdx(int cidx) {
    Chunk c = masterVec().chunkForChunkIdx(cidx);
    if (_indexes!=null) // two way mapping
      return new TransfChunk(c, this);
    else // single way mapping
      return new FlatTransfChunk(c, this);
  }

  static abstract class AbstractTransfChunk extends Chunk {
    protected static final long MISSING_VALUE = -1L;
    final Chunk _c;

    protected AbstractTransfChunk(Chunk c, TransfVec vec) { _c  = c; _len = _c._len; _start = _c._start; _vec = vec; }

    @Override protected double atd_impl(int idx) { double d = 0; return _c.isNA0(idx) ? Double.NaN : ( (d=at8_impl(idx)) == MISSING_VALUE ? Double.NaN : d ) ;  }
    @Override protected boolean isNA_impl(int idx) {
      if (_c.isNA_impl(idx)) return true;
      return at8_impl(idx) == MISSING_VALUE; // this case covers situation when there is no mapping
    }

    @Override boolean set_impl(int idx, long l)   { return false; }
    @Override boolean set_impl(int idx, double d) { return false; }
    @Override boolean set_impl(int idx, float f)  { return false; }
    @Override boolean setNA_impl(int idx)         { return false; }
    @Override boolean hasFloat() { return _c.hasFloat(); }
    @Override NewChunk inflate_impl(NewChunk nc)     { throw new UnsupportedOperationException(); }
    @Override public AutoBuffer write(AutoBuffer bb) { throw new UnsupportedOperationException(); }
    @Override public Chunk read(AutoBuffer bb)       { throw new UnsupportedOperationException(); }

  }

  static class TransfChunk extends AbstractTransfChunk {
    /** @see TransfVec#_values */
    final int[] _values;
    /** @see TransfVec#_indexes */
    final int[] _indexes;
    public TransfChunk(Chunk c, TransfVec vec) {
      super(c,vec);
      assert vec._indexes != null : "TransfChunk needs not-null indexing array.";
      _values = vec._values;
      _indexes = vec._indexes;
    }

    @Override protected long at8_impl(int idx) { return get(_c.at8_impl(idx)); }

    private long get(long val) {
      int indx = -1;
      return (indx = Arrays.binarySearch(_values, (int)val)) < 0 ? MISSING_VALUE : _indexes[indx];
    }
  }

  static class FlatTransfChunk extends AbstractTransfChunk {
    /** @see TransfVec#_values */
    final int[] _values;

    public FlatTransfChunk(Chunk c, TransfVec vec) {
      super(c,vec);
      assert vec._indexes == null : "TransfChunk requires NULL indexing array.";
      _values = vec._values;
    }

    @Override protected long at8_impl(int idx) { return get(_c.at8_impl(idx)); }

    private long get(long val) {
      int indx = -1;
      return (indx = Arrays.binarySearch(_values, (int)val)) < 0 ? MISSING_VALUE : indx ;
    }
  }

  /** Compose this vector with given transformation. Always return a new vector */
  public Vec compose(int[][] transfMap, String[] domain) { return compose(this, transfMap, domain, true);  }

  /**
   * Compose given origVector with given transformation. Always returns a new vector.
   * Original vector is kept if keepOrig is true.
   * @param origVec
   * @param transfMap
   * @param keepOrig
   * @return a new instance of {@link TransfVec} composing transformation of origVector and tranfsMap
   */
  public static Vec compose(TransfVec origVec, int[][] transfMap, String[] domain, boolean keepOrig) {
    // Do a mapping from INT -> ENUM -> this vector ENUM
    int[][] domMap = Utils.compose(new int[][] {origVec._values, origVec._indexes }, transfMap);
    Vec result = origVec.masterVec().makeTransf(domMap[0], domMap[1], domain);;
    if (!keepOrig) DKV.remove(origVec._key);
    return result;
  }
}
