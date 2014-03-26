package water.parser;

import org.junit.Assert;
import org.junit.Test;

import water.*;
import water.fvec.FVecTest;
import water.fvec.ParseDataset2;

public class ParserTest extends TestUtil {

  private double[] d(double... ds) { return ds; }
  private String[] s(String...ss) { return ss; }
  private final double NaN = Double.NaN;
  private final char[] SEPARATORS = new char[] {',', ' '};

  private Key k(String kname, String... data) {
    Key[] keys = new Key[data.length];
    int[] rpc  = new int[data.length];
    for( int i = 0; i < data.length; ++i)
      rpc[i] = data[i].length();
    Key k = Key.make(kname);
    ValueArray va = new ValueArray(k, rpc, 1, new ValueArray.Column[]{new ValueArray.Column(1)});
    DKV.put(k, va);
    for (int i = 0; i < data.length; ++i) {
      keys[i] = va.getChunkKey(i);
      DKV.put(keys[i], new Value(keys[i], data[i]));
    }
    DKV.write_barrier();
    return k;
  }

  public static boolean compareDoubles(double a, double b, double threshold){
    if( a==b ) return true;
    if( ( Double.isNaN(a) && !Double.isNaN(b)) ||
        (!Double.isNaN(a) &&  Double.isNaN(b)) ) return false;
    if( Double.isInfinite(a) || Double.isInfinite(b) ) return false;
    return Math.abs(a-b)/Math.max(Math.abs(a),Math.abs(b)) < threshold;
  }
  public static void testParsed(Key k, double[][] expected, Key inputkey) {
    testParsed(k,expected,inputkey,expected.length);
  }
  public static void testParsed(Key k, double[][] expected, Key inputkey, int len) {
    ValueArray va = DKV.get(k).get();
    Assert.assertEquals(len,va._numrows);
    Assert.assertEquals(expected[0].length,va._cols.length);
    for (int i = 0; i < expected.length; ++i)
      for (int j = 0; j < va._cols.length; ++j) {
        double pval = va.datad(i, j);
        if (Double.isNaN(expected[i][j]))
          Assert.assertFalse(i+" -- "+j, !va.isNA(i,j));
        else
          Assert.assertTrue(expected[i][j]+" -- "+va.datad(i,j),compareDoubles(expected[i][j],va.datad(i,j),0.0000001));
      }
    va.delete();
  }

  @Test public void testBasic() {
    String[] data = new String[] {
        "1|2|3\n1|2|3",
        "4|5|6",
        "4|5.2|",
        "asdf|qwer|1",
        "1.1",
        "1.1|2.1|3.4",
    };

    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(1.0, 2.0, 3.0),
        d(4.0, 5.0, 6.0),
        d(4.0, 5.2, NaN),
        d(NaN, NaN, 1.0),
        d(1.1, NaN, NaN),
        d(1.1, 2.1, 3.4),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      StringBuilder sb = new StringBuilder();
      for( int i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\n");
      Key k = Key.make();
      UKV.put(k, new Value(k, sb.toString()));
      Key r1 = Key.make("r1");
      ParseDataset.parse(r1, new Key[]{k});
      testParsed(r1,exp,k);
      sb = new StringBuilder();
      for( int i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\r\n");
      UKV.put(k, new Value(k, sb.toString()));
      Key r2 = Key.make("r2");
      ParseDataset.parse(r2, new Key[]{k});
      testParsed(r2,exp,k);
    }
  }

  @Test public void testChunkBoundaries() {
    String[] data = new String[] {
        "1|2|3\n1|2|3\n",
        "1|2|3\n1|2", "|3\n1|1|1\n",
        "2|2|2\n2|3|", "4\n3|3|3\n",
        "3|4|5\n5",
        ".5|2|3\n5.","5|2|3\n55e-","1|2.0|3.0\n55e","-1|2.0|3.0\n55","e-1|2.0|3.0\n"

    };
    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(1.0, 2.0, 3.0),
        d(1.0, 2.0, 3.0),
        d(1.0, 2.0, 3.0),
        d(1.0, 1.0, 1.0),
        d(2.0, 2.0, 2.0),
        d(2.0, 3.0, 4.0),
        d(3.0, 3.0, 3.0),
        d(3.0, 4.0, 5.0),
        d(5.5, 2.0, 3.0),
        d(5.5, 2.0, 3.0),
        d(5.5, 2.0, 3.0),
        d(5.5, 2.0, 3.0),
        d(5.5, 2.0, 3.0),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key k = k("ChunkBoundaries",dataset);
      Key r3 = Key.make();
      ParseDataset.parse(r3,new Key[]{k});
      testParsed(r3,exp,k);
    }
  }

  @Test public void testChunkBoundariesMixedLineEndings() {
    String[] data = new String[] {
        "1|2|3\n4|5|6\n7|8|9",
        "\r\n10|11|12\n13|14|15",
        "\n16|17|18\r",
        "\n19|20|21\n",
        "22|23|24\n25|26|27\r\n",
        "28|29|30"
    };
    double[][] exp = new double[][] {
        d(1, 2, 3),
        d(4, 5, 6),
        d(7, 8, 9),
        d(10, 11, 12),
        d(13, 14, 15),
        d(16, 17, 18),
        d(19, 20, 21),
        d(22, 23, 24),
        d(25, 26, 27),
        d(28, 29, 30),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key k = k("ChunkBoundariesMixedLineEndings",dataset);
      Key r4 = Key.make();
      ParseDataset.parse(r4,new Key[]{k});
      testParsed(r4,exp,k);
    }
  }

  @Test public void testNondecimalColumns() {
    String data[] = {
          "1| 2|one\n"
        + "3| 4|two\n"
        + "5| 6|three\n"
        + "7| 8|one\n"
        + "9| 10|two\n"
        + "11|12|three\n"
        + "13|14|one\n"
        + "15|16|\"two\"\n"
        + "17|18|\" four\"\n"
        + "19|20| three\n",
    };

    double[][] expDouble = new double[][] {
        d(1, 2, 1), // preserve order
        d(3, 4, 3),
        d(5, 6, 2),
        d(7, 8, 1),
        d(9, 10, 3),
        d(11,12, 2),
        d(13,14, 1),
        d(15,16, 3),
        d(17,18, 0),
        d(19,20, 2),
    };

    String[][] expString = new String[][] {
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, "three"),
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, "three"),
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, " four"),
        s(null,null, "three"),
    };

    String expDomain[] = s( "one", "two", "three", " four" );

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key key = k("NondecimalColumns",dataset);
      Key r = Key.make();
      ParseDataset.parse(r,new Key[]{key});
      ValueArray va = DKV.get(r).get();
      String[] cd = va._cols[2]._domain;
      Assert.assertEquals(" four",cd[0]);
      Assert.assertEquals("one",cd[1]);
      Assert.assertEquals("three",cd[2]);
      Assert.assertEquals("two",cd[3]);
      testParsed(r, expDouble,key);
    }
  }

  @Test public void testNumberFormats(){
    String [] data = {"+.6e102|+.7e102|+.8e102\n.6e102|.7e102|.8e102\n"};
    double[][] expDouble = new double[][] {
        d(+.6e102,.7e102,.8e102), // preserve order
        d(+.6e102, +.7e102,+.8e102),
    };
    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key key = k("NumberFormats",dataset);
      Key r = Key.make();
      ParseDataset.parse(r,new Key[]{key});
      testParsed(r, expDouble,key);
    }
  }
 @Test public void testMultipleNondecimalColumns() {
    String data[] = {
        "foo| 2|one\n"
      + "bar| 4|two\n"
      + "foo| 6|three\n"
      + "bar| 8|one\n"
      + "bar|ten|two\n"
      + "bar| 12|three\n"
      + "foobar|14|one\n",
    };
    double[][] expDouble = new double[][] {
        d(1, 2, 0), // preserve order
        d(0, 4, 2),
        d(1, 6, 1),
        d(0, 8, 0),
        d(0, NaN, 2),
        d(0, 12, 1),
        d(2, 14, 0),
    };


    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key key = k("MultipleNondecimalColumns",dataset);
      Key r = Key.make();
      ParseDataset.parse(r,new Key[]{key});
      ValueArray va = DKV.get(r).get();
      String[] cd = va._cols[2]._domain;
      Assert.assertEquals("one",cd[0]);
      Assert.assertEquals("three",cd[1]);
      Assert.assertEquals("two",cd[2]);
      cd = va._cols[0]._domain;
      Assert.assertEquals("bar",cd[0]);
      Assert.assertEquals("foo",cd[1]);
      Assert.assertEquals("foobar",cd[2]);
      testParsed(r, expDouble,key);
    }
  }


  // Test if the empty column is correctly handled.
  // NOTE: this test makes sense only for comma separated columns
  @Test public void testEmptyColumnValues() {
    String data[] = {
        "1,2,3,foo\n"
      + "4,5,6,bar\n"
      + "7,,8,\n"
      + ",9,10\n"
      + "11,,,\n"
      + "0,0,0,z\n"
      + "0,0,0,z\n"
      + "0,0,0,z\n"
      + "0,0,0,z\n"
      + "0,0,0,z\n"
    };
    double[][] expDouble = new double[][] {
        d(1, 2, 3, 1),
        d(4, 5, 6, 0),
        d(7, NaN, 8, NaN),
        d(NaN, 9, 10, NaN),
        d(11, NaN, NaN, NaN),
        d(0, 0, 0, 2),
        d(0, 0, 0, 2),
        d(0, 0, 0, 2),
        d(0, 0, 0, 2),
        d(0, 0, 0, 2),
    };

    final char separator = ',';

    String[] dataset = getDataForSeparator(separator, data);
    Key key = k("EmptyColumnValues",dataset);
    Key r = Key.make();
    ParseDataset.parse(r,new Key[]{key});
    ValueArray va = DKV.get(r).get();
    String[] cd = va._cols[3]._domain;
    Assert.assertEquals("bar",cd[0]);
    Assert.assertEquals("foo",cd[1]);
    testParsed(r, expDouble,key);
  }


  @Test public void testBasicSpaceAsSeparator() {

    String[] data = new String[] {
        " 1|2|3",
        " 4 | 5 | 6",
        "4|5.2 ",
        "asdf|qwer|1",
        "1.1",
        "1.1|2.1|3.4",
    };
    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(4.0, 5.0, 6.0),
        d(4.0, 5.2, NaN),
        d(NaN, NaN, 1.0),
        d(1.1, NaN, NaN),
        d(1.1, 2.1, 3.4),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      int i = 0;
      StringBuilder sb = new StringBuilder();
      for( i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\n");
      Key k = Key.make();
      UKV.put(k, new Value(k, sb.toString()));
      Key r5 = Key.make();
      ParseDataset.parse(r5, new Key[]{k});
      testParsed(r5, exp,k);
    }
  }

  String[] getDataForSeparator(char sep, String[] data) {
    return getDataForSeparator('|', sep, data);
  }
  String[] getDataForSeparator(char placeholder, char sep, String[] data) {
    String[] result = new String[data.length];
    for (int i = 0; i < data.length; i++) {
      result[i] = data[i].replace(placeholder, sep);
    }
    return result;
  }

  @Test public void testTimeParse() {
    Key fkey = load_test_file("smalldata/kaggle/bestbuy_train_10k.csv.gz");
    Key okey = Key.make("bestbuy.hex");
    ParseDataset.parse(okey,new Key[]{fkey});
    ValueArray va = DKV.get(okey).get();
    va.delete();
  }

  @Test public void testMixedSeps() {
    double[][] exp = new double[][] {
      d(NaN,   1,   1),
      d(NaN,   2, NaN),
      d(  3, NaN,   3),
      d(  4, NaN, NaN),
      d(NaN, NaN, NaN),
      d(NaN, NaN, NaN),
      d(NaN, NaN,   6),
    };
    Key fkey = load_test_file("smalldata/test/is_NA.csv");
    Key okey = Key.make("NA.hex");
    ParseDataset.parse(okey,new Key[]{fkey});
    testParsed(okey,exp,fkey,25);
  }

  @Test public void testSVMLight() {
    String[] data = new String[] {
        " 1 2:.2 5:.5 9:.9\n",
        "-1 7:.7 8:.8 9:.9\n",
        "+1 1:.1 5:.5 6:.6\n"
    };

    double[][] exp = new double[][] {
        d( 1., .0, .2, .0, .0, .5, .0, .0, .0, .9),
        d(-1., .0, .0, .0, .0, .0, .0, .7, .8, .9),
        d( 1., .1, .0, .0, .0, .5, .6, .0, .0, .0),
    };
    String[] dataset = data;
    StringBuilder sb = new StringBuilder();
    for( int i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\n");
    Key k = k(Key.make().toString(),sb.toString());
    Key r1 = Key.make("r1");
    ParseDataset.parse(r1, new Key[]{k});
    testParsed(r1,exp,k);
  }

  // Mix of NA's, very large & very small, ^A Hive-style seperator, comments, labels
  @Test public void testParseMix() {
    Key fkey = load_test_file("smalldata/test/test_parse_mix.csv");
    Key okey = Key.make("mix.hex");
    ParseDataset.parse(okey,new Key[]{fkey});

    double[][] exp = new double[][] {
      d( 0      ,  0.5    ,  1      , 0),
      d( 3      ,  NaN    ,  4      , 1),
      d( 6      ,  NaN    ,  8      , 0),
      d( 0.6    ,  0.7    ,  0.8    , 1),
      d(+0.6    , +0.7    , +0.8    , 0),
      d(-0.6    , -0.7    , -0.8    , 1),
      d(  .6    ,   .7    ,   .8    , 0),
      d(+ .6    ,  +.7    ,  +.8    , 1),
      d(- .6    ,  -.7    ,  -.8    , 0),
      d(+0.6e0  , +0.7e0  , +0.8e0  , 1),
      d(-0.6e0  , -0.7e0  , -0.8e0  , 0),
      d(  .6e0  ,   .7e0  ,   .8e0  , 1),
      d(+ .6e0  ,  +.7e0  ,  +.8e0  , 0),
      d( -.6e0  ,  -.7e0  ,  -.8e0  , 1),
      d(+0.6e00 , +0.7e00 , +0.8e00 , 0),
      d(-0.6e00 , -0.7e00 , -0.8e00 , 1),
      d(  .6e00 ,   .7e00 ,   .8e00 , 0),
      d( +.6e00 ,  +.7e00 ,  +.8e00 , 1),
      d( -.6e00 ,  -.7e00 ,  -.8e00 , 0),
      d(+0.6e-01, +0.7e-01, +0.8e-01, 1),
      d(-0.6e-01, -0.7e-01, -0.8e-01, 0),
      d(  .6e-01,   .7e-01,   .8e-01, 1),
      d( +.6e-01,  +.7e-01,  +.8e-01, 0),
      d( -.6e-01,  -.7e-01,  -.8e-01, 1),
      d(+0.6e+01, +0.7e+01, +0.8e+01, 0),
      d(-0.6e+01, -0.7e+01, -0.8e+01, 1),
      d(  .6e+01,   .7e+01,   .8e+01, 0),
      d( +.6e+01,  +.7e+01,  +.8e+01, 1),
      d( -.6e+01,  -.7e+01,  -.8e+01, 0),
      d(+0.6e102, +0.7e102, +0.8e102, 1),
      d(-0.6e102, -0.7e102, -0.8e102, 0),
      d(  .6e102,   .7e102,   .8e102, 1),
      d( +.6e102,  +.7e102,  +.8e102, 0),
      d( -.6e102,  -.7e102,  -.8e102, 1)
    };
    testParsed(okey, exp, fkey);
  }
}
