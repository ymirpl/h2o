package hex.drf;

import org.junit.BeforeClass;
import org.junit.Test;
import water.TestUtil;
import water.fvec.Frame;
import water.UKV;

public class DRFTest2 extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  static final String[] s(String...arr)  { return arr; }
  static final long[]   a(long ...arr)   { return arr; }
  static final long[][] a(long[] ...arr) { return arr; }

  // A bigger DRF test, useful for tracking memory issues.
  /*@Test*/ public void testAirlines() throws Throwable {
    for( int i=0; i<10; i++ ) {
      new DRFTest().basicDRF(
        //
        //"../demo/c5/row10000.csv.gz", "c5.hex", null, null, 

        "../datasets/UCI/UCI-large/covtype/covtype.data", "covtype.hex", null, null,
        new DRFTest.PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
        10/*ntree*/,
        a( a( 199019,   7697,    15,    0,  180,    45,   546), 
           a(   8012, 267788,   514,    7,  586,   329,   181), 
           a(     16,    707, 33424,  162,   53,   639,     0), 
           a(      1,      5,   353, 2211,    0,    99,     0), 
           a(    181,   1456,   134,    0, 7455,    43,     4), 
           a(     30,    540,  1171,   96,   33, 15109,     0), 
           a(    865,    167,     0,    0,    9,     0, 19075)),
        s("1", "2", "3", "4", "5", "6", "7"),

        //"./smalldata/iris/iris_wheader.csv", "iris.hex", null, null,
        //new DRFTest.PrepData() { @Override int prep(Frame fr) { return fr.numCols()-1; } },
        //10/*ntree*/,
        //a( a( 50,  0,  0),
        //   a(  0, 50,  0),
        //   a(  0,  0, 50)),
        //s("Iris-setosa","Iris-versicolor","Iris-virginica"),

        //"./smalldata/logreg/prostate.csv", "prostate.hex", null, null,
        //new DRFTest.PrepData() { @Override int prep(Frame fr) {
        //  UKV.remove(fr.remove("ID")._key); return fr.find("CAPSULE");
        //  } },
        //10/*ntree*/,
        //a( a(170, 55),
        //   a( 60, 92)),
        //s("0","1"),


        99/*max_depth*/,
        20/*nbins*/,
        0 /*optflag*/  );
    }
  }
}
