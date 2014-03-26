package water.fvec;

import water.AutoBuffer;
import water.MemoryManager;
import water.H2O;

/** A simple chunk for boolean values. In fact simple bit vector.
 * Each boolean is represented by 2bits since we need to represent
 * NA.
 */
public class CBSChunk extends Chunk {
  static protected final byte _NA  = 0x02; // Internal representation of NA
  static final int OFF = 2;
  protected byte _bpv;
  protected byte _gap;// number of trailing unused bits in the end (== _len % 8, we allocate bytes, but our length i generally not multiple of 8)
  public CBSChunk(byte[] bs, byte gap, byte bpv) {
    assert gap < 8; assert bpv == 1 || bpv == 2;
    _mem = bs; _start = -1; _gap = gap; _bpv = bpv;
    _len = ((_mem.length - OFF)*8 - _gap) / _bpv; // number of boolean items
  }
  @Override protected long at8_impl(int idx) {
    byte b = atb(idx);
    if( b == _NA ) throw new IllegalArgumentException("at8 but value is missing");
    return b;
  }
  @Override protected double atd_impl(int idx) {
    byte b = atb(idx);
    return b == _NA ? Double.NaN : b;
  }
  @Override protected final boolean isNA_impl( int i ) { return atb(i)==_NA; }
  protected byte atb(int idx) {
    int vpb = 8 / _bpv;  // values per byte
    int bix = OFF + idx / vpb; // byte index
    int off = _bpv * (idx % vpb);
    byte b   = _mem[bix];
    switch( _bpv ) {
      case 1: return read1b(b, off);
      case 2: return read2b(b, off);
      default: H2O.fail();
    }
    return -1;
  }
  @Override boolean set_impl(int idx, long l)   { return false; }
  @Override boolean set_impl(int idx, double d) { return false; }
  @Override boolean set_impl(int idx, float f ) { return false; }
  @Override boolean setNA_impl(int idx) { 
    if( _bpv == 1 ) return false;
    throw H2O.unimpl();
  }
  @Override boolean hasFloat ()                  { return false; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem, _mem.length); }
  @Override public Chunk read(AutoBuffer bb) {
    _mem   = bb.bufClose();
    _start = -1;
    _gap   = _mem[0];
    _bpv   = _mem[1];
    _len = ((_mem.length - OFF)*8 - _gap) / _bpv;
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    nc._xs = MemoryManager.malloc4(_len);
    nc._ls = MemoryManager.malloc8(_len);
    for (int i=0; i<_len; i++) {
      int res = atb(i);
      if (res == _NA) nc._xs[i] = Integer.MIN_VALUE;
      else            nc._ls[i] = res;
    }
    return nc;
  }

  /** Writes 1bit from value into b at given offset and return b */
  public static byte write1b(byte b, byte val, int off) {
    val = (byte) ((val & 0x1) << (7-off));
    return (byte) (b | val);
  }
  /** Writes 2bits from value into b at given offset and return b */
  public static byte write2b(byte b, byte val, int off) {
    val = (byte) ((val & 0x3) << (6-off)); // 0000 00xx << (6-off)
    return (byte) (b | val);
  }

  /** Reads 1bit from given b in given offset. */
  public static byte read1b(byte b, int off) { return (byte) ((b >> (7-off)) & 0x1); }
  /** Reads 1bit from given b in given offset. */
  public static byte read2b(byte b, int off) { return (byte) ((b >> (6-off)) & 0x3); }

  /** Returns compressed len of the given array length if the value if represented by bpv-bits. */
  public static int clen(int values, int bpv) {
    int len = (values*bpv) >> 3;
    return values*bpv % 8 == 0 ? len : len + 1;
  }
}
