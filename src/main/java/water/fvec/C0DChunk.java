package water.fvec;

import java.util.Arrays;
import water.AutoBuffer;
import water.MemoryManager;
import water.UDP;

/**
 * The constant 'double' column.
 */
public class C0DChunk extends Chunk {
  static final int OFF=8+4;
  double _con;
  public C0DChunk(double con, int len) { _mem=new byte[OFF]; _start = -1; _len = len;
    _con = con;
    UDP.set8d(_mem,0,con);
    UDP.set4(_mem,8,len);
  }
  @Override protected final long at8_impl( int i ) {
    if( Double.isNaN(_con) ) throw new IllegalArgumentException("at8 but value is missing");
    return (long)_con;          // Possible silent truncation
  }
  @Override protected final double atd_impl( int i ) {return _con;}
  @Override protected final boolean isNA_impl( int i ) { return Double.isNaN(_con); }
  @Override boolean set_impl(int idx, long l) { return l==_con; }
  @Override boolean set_impl(int i, double d) { return d==_con; }
  @Override boolean set_impl(int i, float f ) { return f==_con; }
  @Override boolean setNA_impl(int i) { return Double.isNaN(_con); }
  @Override boolean hasFloat() { return (long)_con!=_con; }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb.putA1(_mem,_mem.length); }
  @Override public C0DChunk read(AutoBuffer bb) {
    _mem = bb.bufClose();
    _start = -1;
    _con = UDP.get8d(_mem,0);
    _len = UDP.get4(_mem,8);
    return this;
  }
  @Override NewChunk inflate_impl(NewChunk nc) {
    Arrays.fill(nc._ds = MemoryManager.malloc8d(_len),_con);
    return nc;
  }
  // 3.3333333e33
  public int pformat_len0() { return 22; }
  public String pformat0() { return "% 21.15e"; }
}