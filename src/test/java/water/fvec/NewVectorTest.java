package water.fvec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.*;

import water.*;
import water.parser.DParseTask;

public class NewVectorTest extends TestUtil {
  static final double EPSILON = 1e-6;
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  private void testImpl( long[] ls, int[] xs, Class C, boolean hasFloat ) {
    AppendableVec av = new AppendableVec(Vec.newKey());
    NewChunk nv = new NewChunk(av,0);
    nv._ls = ls;
    nv._xs = xs;
    nv._len= nv._len2 = ls.length;
    Chunk bv = nv.compress();
    bv._vec = av.close(new Futures());
    // Compression returns the expected compressed-type:
    assertTrue( "Found chunk class "+bv.getClass()+" but expected "+C, C.isInstance(bv) );
    assertEquals( hasFloat, bv.hasFloat() );
    // Also, we can decompress correctly
    for( int i=0; i<ls.length; i++ )
      assertEquals(ls[i]*DParseTask.pow10(xs[i]), bv.at0(i), bv.at0(i)*EPSILON);
    UKV.remove(av._key);
  }
  // Test that various collections of parsed numbers compress as expected.
  @Test public void testCompression() {
    // A simple no-compress
    testImpl(new long[] {120, 12,120},
             new int [] {  0,  1,  0},
             C0LChunk.class,false);
    // A simple no-compress
    testImpl(new long[] {122, 3,44},
             new int [] {  0, 0, 0},
             C1NChunk.class,false);
    // A simple compressed boolean vector
    testImpl(new long[] {1, 0, 1},
             new int [] {0, 0, 0},
             CBSChunk.class,false);
    // Scaled-byte compression
    testImpl(new long[] {122,-3,44}, // 12.2, -3.0, 4.4 ==> 122e-1, -30e-1, 44e-1
             new int [] { -1, 0,-1},
             C1SChunk.class, true);
    // Positive-scale byte compression
    testImpl(new long[] {1000,200,30}, // 1000, 2000, 3000 ==> 1e3, 2e3, 3e3
             new int [] {   0,  1, 2},
             C1SChunk.class,false);
    // A simple no-compress short
    testImpl(new long[] {1000,200,32767, -32767,32},
             new int [] {   0,  1,    0,      0, 3},
             C2Chunk.class,false);
    // Scaled-byte compression
    testImpl(new long[] {50100,50101,50123,49999}, // 50100, 50101, 50123, 49999
             new int [] {    0,    0,    0,    0},
             C1SChunk.class,false);
    // Scaled-byte compression
    testImpl(new long[] {51000,50101,50123,49999}, // 51000, 50101, 50123, 49999
             new int [] {    0,    0,    0,    0},
             C2SChunk.class,false);
    // Scaled-short compression
    testImpl(new long[] {501000,501001,50123,49999}, // 50100.0, 50100.1, 50123, 49999
             new int [] {    -1,    -1,    0,    0},
             C2SChunk.class, true);
    // Integers
    testImpl(new long[] {123456,2345678,34567890},
             new int [] {     0,      0,       0},
             C4Chunk.class,false);
//    // Floats
    testImpl(new long[] {1234,2345,314},
             new int [] {  -1,  -5, -2},
             C4SChunk.class, true);
    // Doubles
    testImpl(new long[] {1234,2345678,31415},
             new int [] {  40,     10,  -40},
             C8DChunk.class, true);
  }

  // Testing writes to an existing Chunk causing inflation
  @Test public void testWrites() {
    Key key = Vec.newKey();
    AppendableVec av = new AppendableVec(key);
    NewChunk nv = new NewChunk(av,0);
    nv._ls = new long[]{0,0,0,0}; // A 4-row chunk
    nv._xs = new int []{0,0,0,0};
    nv._len= nv._len2 = nv._ls.length;
    nv.close(0,null);
    Vec vec = av.close(new Futures());
    assertEquals( nv._ls.length, vec.length() );
    // Compression returns the expected constant-compression-type:
    Chunk c0 = vec.chunkForChunkIdx(0);
    assertTrue( "Found chunk class "+c0.getClass()+" but expected C0LChunk", c0 instanceof C0LChunk );
    assertEquals( false, c0.hasFloat() );
    // Also, we can decompress correctly
    for( int i=0; i<nv._ls.length; i++ )
      assertEquals(0, c0.at0(i), c0.at0(i)*EPSILON);

    // Now write a zero into slot 0
    vec.set(0,0);
    assertEquals(0,vec.at8(0));
    Chunk c1 = vec.chunkForChunkIdx(0);
    assertTrue( "Found chunk class "+c1.getClass()+" but expected C0LChunk", c1 instanceof C0LChunk );

    // Now write a one into slot 1; chunk should inflate into boolean vector.
    c1.set(1,1);
    assertEquals(1,vec.at8(1)); // Immediate visibility in current thread
    c1.close(0,null);           // Done writing into chunk
    Chunk c2 = vec.chunkForChunkIdx(0);  // Look again at the installed chunk
    assertTrue( "Found chunk class "+c2.getClass()+" but expected CBSChunk", c2 instanceof CBSChunk );

    // Now write a two into slot 2; chunk should inflate into byte vector
    c2.set(2,2);
    assertEquals(2,vec.at8(2)); // Immediate visibility in current thread
    c2.close(0,null);           // Done writing into chunk
    Chunk c3 = vec.chunkForChunkIdx(0);  // Look again at the installed chunk
    assertTrue( "Found chunk class "+c3.getClass()+" but expected C1NChunk", c3 instanceof C1NChunk );

    c3.set(3,3);
    assertEquals(3,vec.at8(3)); // Immediate visibility in current thread
    c3.close(0,null);           // Done writing into chunk
    Chunk c4 = vec.chunkForChunkIdx(0);  // Look again at the installed chunk
    assertTrue( "Found chunk class "+c4.getClass()+" but expected C1NChunk", c4 instanceof C1NChunk );

    UKV.remove(av._key);
  }
}
