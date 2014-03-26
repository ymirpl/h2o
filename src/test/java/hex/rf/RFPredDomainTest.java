package hex.rf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import hex.rf.ConfusionTask.CMFinal;
import hex.rf.ConfusionTask.CMJob;
import hex.rf.DRF.DRFJob;
import hex.rf.Tree.StatType;

import org.junit.*;

import water.*;

/**
 * Note: This test expect cloud of 3 nodes.
 * However, since data are small, random forest will be run only one one node containing
 * data.
 *
 * Hence, the test can be run on single node as well and all results shoudl match.
 */
public class RFPredDomainTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(2); }

  static final long[] a(long ...p) { return p; }

  static void runIrisRF(final String trainDS, final String testDS, double expTestErr, long[][] expCM, String[] expDomain) throws Exception {
    String trainKeyName = "iris_train.hex";
    Key trainKey        = loadAndParseFile(trainKeyName, trainDS);
    ValueArray trainData = DKV.get(trainKey).get();

    int  trees    = 10;
    int  depth    = 50;
    StatType  statType = StatType.ENTROPY;
    long seed     = 0xae44a87f9edf1cbL;
    int[] cols    = new int[]{0,1,2,3,4};

    // Start the distributed Random Forest
    String modelName   = "model";
    final Key modelKey = Key.make(modelName);

    DRFJob drf = hex.rf.DRF.execute(modelKey,cols,trainData,trees,depth,1024,statType,seed,false, null, -1, Sampling.Strategy.RANDOM, 1.0f, null, 0, 0, false);
    // Block for completion
    RFModel model = drf.get();

    String testKeyName  = "iris_test.hex";
    // Load validation dataset
    Key testKey         = loadAndParseFile(testKeyName, testDS);
    ValueArray testData = DKV.get(testKey).get();
    Job cmJob = ConfusionTask.make(model, testData._key, model._features-1, null, false);
    CMFinal cm = cmJob.get();
    cm.report();
    assertEquals("Error rate", expTestErr, cm.classError(), 0.001);
    assertEquals("CF dimension", expCM.length, cm._matrix.length);
    for (int i = 0; i<expCM.length; i++) {
      assertArrayEquals(expCM[i], cm._matrix[i]);
    }
    assertArrayEquals("Confusion matrix", expCM, cm._matrix);
    assertArrayEquals("CM domain", expDomain, cm.domain());

    model.delete();
    testData.delete();
    trainData.delete();
    UKV.remove(cmJob.dest());
  }

  /**
   * Scenario:
   *   model: A B C
   *   data : A B C D E
   *   CM   : A B C D E
   */
  @Test
  public void irisExtra() throws Exception {
    long[][] cm = new long[][] {
        a(0, 0,  0, 0,  1),
        a(0, 20, 0, 0,  0),
        a(0, 0,  0, 1,  0),
        a(0, 0,  0, 16, 0),
        a(0, 0,  0, 1,  13)
    };
    String[] cmDomain = new String[] {"Iris-borovicka", "Iris-setosa", "Iris-slivovicka", "Iris-versicolor", "Iris-virginica" };

    runIrisRF("smalldata/test/classifier/iris_train.csv",
              "smalldata/test/classifier/iris_test_extra.csv",
              0.057f, cm, cmDomain);
  }

  /**
   * Scenario:
   *   model: A B C
   *   data :   B
   *   CM   : A B C
   */
  @Test
  @Ignore
  public void irisMissing() throws Exception {
    long[][] cm = new long[][] {
        a(0, 0,  0),
        a(0, 16, 0),
        a(0, 0,  0),
    };
    String[] cmDomain = new String[] {"Iris-setosa", "Iris-versicolor", "Iris-virginica" };

    runIrisRF("smalldata/test/classifier/iris_train.csv",
              "smalldata/test/classifier/iris_test_missing.csv",
              0.0f, cm, cmDomain);
  }

  /**
   * Scenario:
   *   model: A B C
   *   data :   B   D E
   *   CM   : A B C D E
   */
  @Test
  public void irisMissingAndExtra() throws Exception {
    long[][] cm = new long[][] {
        a(0, 0,  0, 0, 1),
        a(0, 0,  0, 0, 0),
        a(0, 0,  0, 1, 0),
        a(0, 0,  0,16, 0),
        a(0, 0,  0, 0, 0),
    };
    String[] cmDomain = new String[] {"Iris-borovicka", "Iris-setosa", "Iris-slivovicka", "Iris-versicolor", "Iris-virginica" };

    runIrisRF("smalldata/test/classifier/iris_train.csv",
              "smalldata/test/classifier/iris_test_missing_extra.csv",
              0.111f, cm, cmDomain);
  }


  /**
   * Scenario:
   *   model: [0,2]
   *   data : [0,4]
   *   CM   : [0,4]
   */
  @Test
  @Ignore
  public void irisNumericExtra() throws Exception {
    long[][] cm = new long[][] {
        a(20, 0,  0,  0, 0),
        a(0,  16, 0,  0, 0),
        a(0,  1,  13, 0, 0),
        a(0,  0,  1,  0, 0),
        a(0,  1,  0,  0, 0),
    };
    String[] cmDomain = new String[] {"0", "1", "2", "3", "4" };

    runIrisRF("smalldata/test/classifier/iris_train_numeric.csv",
              "smalldata/test/classifier/iris_test_numeric_extra.csv",
              0.057f, cm, cmDomain);
  }

  /**
   * Scenario:
   *   model: [0,2]
   *   data : [2,2]
   *   CM   : [0,2]
   */
  @Test
  public void irisNumericMissing() throws Exception {
    long[][] cm = new long[][] {
        a(0, 0, 0),
        a(0, 16,  0),
        a(0, 0,  0),
    };
    String[] cmDomain = new String[] {"0", "1", "2" };

    runIrisRF("smalldata/test/classifier/iris_train_numeric.csv",
              "smalldata/test/classifier/iris_test_numeric_missing.csv",
              0.0f, cm, cmDomain);
  }

  /**
   * Scenario:
   *   model: [0,2]
   *   data : [0,4]\{0}\{2}
   *   CM   : [0,4]
   */
  @Test
  public void irisNumericMissingAndExtra() throws Exception {
    long[][] cm = new long[][] {
        a(0, 0,  0, 0, 0),
        a(0, 16, 0, 0, 0),
        a(0, 0,  0, 0, 0),
        a(0, 0,  1, 0, 0),
        a(0, 1,  0, 0, 0),
    };
    String[] cmDomain = new String[] {"0", "1", "2", "3", "4"};
    runIrisRF("smalldata/test/classifier/iris_train_numeric.csv",
              "smalldata/test/classifier/iris_test_numeric_missing_extra.csv",
              0.111f, cm, cmDomain);
  }

  /**
   * Scenario:
   *   model: [0,2]
   *   data : [-1,4]
   *   CM   : [-1,4]
   */
  @Test
  public void irisNumericExtra2() throws Exception {
    long[][] cm = new long[][] {
        a(0, 0,  0, 1, 0, 0),
        a(0, 20, 0, 0, 0, 0),
        a(0, 0, 16, 0, 0, 0),
        a(0, 0,  1,13, 0, 0),
        a(0, 0,  0, 0, 0, 0),
        a(0, 0,  1, 0, 0, 0),
    };
    String[] cmDomain = new String[] {"-1", "0", "1", "2", "3", "4"};
    runIrisRF("smalldata/test/classifier/iris_train_numeric.csv",
              "smalldata/test/classifier/iris_test_numeric_extra2.csv",
              0.057f, cm, cmDomain);
  }

  /**
   * Scenario:
   *   model: [0,2]
   *   data : [-1,4]
   *   CM   : [-1,4]
   */
  @Test
  @Ignore // ignore for now
  public void irisExtraWithNAs() throws Exception {
    long[][] cm = new long[][] {
        a(0, 0,  0, 1, 0, 0),
        a(0, 20, 0, 0, 0, 0),
        a(0, 0, 16, 0, 0, 0),
        a(0, 0,  1,13, 0, 0),
        a(0, 0,  0, 0, 0, 0),
        a(0, 0,  1, 0, 0, 0),
    };
    String[] cmDomain = new String[] {"-1", "0", "1", "2", "3", "4"};
    runIrisRF("smalldata/test/classifier/iris_train.csv",
              "smalldata/test/classifier/iris_test_extra_with_na.csv",
              0.057f, cm, cmDomain);
  }
}
