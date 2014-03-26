package water;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.File;
import java.util.concurrent.ExecutionException;

import jsr166y.CountedCompleter;

import org.junit.*;

import water.DException.DistributedException;

public class MRThrow extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }


  // ---
  // Map in h2o.jar - a multi-megabyte file - into Arraylets.
  // Run a distributed byte histogram.  Throw an exception in *some* map call,
  // and make sure it's forwarded to the invoke.
  @Test public void testInvokeThrow() {
    File file = find_test_file("target/h2o.jar");
    Key h2okey = load_test_file(file);
    try {
      for(int i = 0; i < H2O.CLOUD._memary.length; ++i){
        ByteHistoThrow bh = new ByteHistoThrow();
        bh._throwAt = H2O.CLOUD._memary[i].toString();
        try {
          bh.invoke(h2okey); // invoke should throw DistributedException wrapped up in RunTimeException
          fail("should've thrown");
        }catch(RuntimeException e){
          assertTrue(e.getMessage().indexOf("test") != -1);
        } catch(Throwable ex){
          ex.printStackTrace();
          fail("Expected RuntimeException, got " + ex.toString());
        }
      }
    } finally {
      Lockable.delete(h2okey);
    }
  }

  @Test public void testGetThrow() {
    File file = find_test_file("target/h2o.jar");
    Key h2okey = load_test_file(file);
    try {
      for(int i = 0; i < H2O.CLOUD._memary.length; ++i){
        ByteHistoThrow bh = new ByteHistoThrow();
        bh._throwAt = H2O.CLOUD._memary[i].toString();
        try {
          bh.dfork(h2okey).get(); // invoke should throw DistributedException wrapped up in RunTimeException
          fail("should've thrown");
        }catch(ExecutionException e){
          assertTrue(e.getMessage().indexOf("test") != -1);
        } catch(Throwable ex){
          ex.printStackTrace();
          fail("Expected ExecutionException, got " + ex.toString());
        }
      }
    } finally {
      Lockable.delete(h2okey);
    }
  }

  @Test public void testContinuationThrow() throws InterruptedException, ExecutionException {
    File file = find_test_file("target/h2o.jar");
    Key h2okey = load_test_file(file);
    try {
      for(int i = 0; i < H2O.CLOUD._memary.length; ++i){
        ByteHistoThrow bh = new ByteHistoThrow();
        bh._throwAt = H2O.CLOUD._memary[i].toString();
        System.out.println("RUNNING NODE:" + bh._throwAt);
        final boolean [] ok = new boolean[]{false};
        try {
          bh.setCompleter(new CountedCompleter() {
            @Override public void compute() {}
            @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
              ok[0] = ex.getMessage().indexOf("test") != -1;
              return true;
            }
          });
          bh.dfork(h2okey).get(); // invoke should throw DistrDTibutedException wrapped up in RunTimeException
          assertTrue(ok[0]);
        }catch(ExecutionException eex){
          assertTrue(eex.getCause().getMessage().indexOf("test") != -1);
        } catch(Throwable ex){
          ex.printStackTrace();
          fail("Unexpected exception" + ex.toString());
        }
      }
    } finally {
      Lockable.delete(h2okey);
    }
  }

//  @Test public void testDTask(){
//    for(int i = 0; i < H2O.CLOUD._memary.length; ++i){
//      try{
//        RPC.call(H2O.CLOUD._memary[i], new DTask<DTask>() {
//          @Override public void compute2() {
//            throw new RuntimeException("test");
//          }
//        }).get();
//        fail("should've thrown");
//      }catch(RuntimeException rex){
//       assertTrue(rex.getCause().getMessage().equals("test"));
//      } catch(Throwable t){
//        fail("Expected RuntimException");
//      }
//    }
//  }

  // Byte-wise histogram
  public static class ByteHistoThrow extends MRTask<ByteHistoThrow> {
    int[] _x;
    String _throwAt;
    // Count occurrences of bytes
    @SuppressWarnings("divzero")
    public void map( Key key ) {
      _x = new int[256];        // One-time set histogram array
      Value val = DKV.get(key); // Get the Value for the Key
      byte[] bits = val.memOrLoad();  // Compute local histogram
      for( int i=0; i<bits.length; i++ )
        _x[bits[i]&0xFF]++;
      if(H2O.SELF.toString().equals(_throwAt))
        throw new RuntimeException("test");
    }
    // ADD together all results
    public void reduce( ByteHistoThrow bh ) {
      if( _x == null ) { _x = bh._x; return; }
      for( int i=0; i<_x.length; i++ )
        _x[i] += bh._x[i];
    }
  }
}
