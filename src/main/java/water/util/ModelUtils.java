package water.util;

import java.util.Arrays;
import java.util.Random;

import water.H2O;

/**
 * Shared static code to support modeling, prediction, and scoring.
 *
 *  Used by interpreted models as well as by generated model code.
 *
 */
public class ModelUtils {

  /** List of default thresholds */
  public static float[] DEFAULT_THRESHOLDS = new float [] {  0.00f,
    0.01f, 0.02f, 0.03f, 0.04f, 0.05f, 0.06f, 0.07f, 0.08f, 0.09f, 0.10f,
    0.11f, 0.12f, 0.13f, 0.14f, 0.15f, 0.16f, 0.17f, 0.18f, 0.19f, 0.20f,
    0.21f, 0.22f, 0.23f, 0.24f, 0.25f, 0.26f, 0.27f, 0.28f, 0.29f, 0.30f,
    0.31f, 0.32f, 0.33f, 0.34f, 0.35f, 0.36f, 0.37f, 0.38f, 0.39f, 0.40f,
    0.41f, 0.42f, 0.43f, 0.44f, 0.45f, 0.46f, 0.47f, 0.48f, 0.49f, 0.50f,
    0.51f, 0.52f, 0.53f, 0.54f, 0.55f, 0.56f, 0.57f, 0.58f, 0.59f, 0.60f,
    0.61f, 0.62f, 0.63f, 0.64f, 0.65f, 0.66f, 0.67f, 0.68f, 0.69f, 0.70f,
    0.71f, 0.72f, 0.73f, 0.74f, 0.75f, 0.76f, 0.77f, 0.78f, 0.79f, 0.80f,
    0.81f, 0.82f, 0.83f, 0.84f, 0.85f, 0.86f, 0.87f, 0.88f, 0.89f, 0.90f,
    0.91f, 0.92f, 0.93f, 0.94f, 0.95f, 0.96f, 0.97f, 0.98f, 0.99f, 1.00f
  };

  /**
   *  Utility function to get a best prediction from an array of class
   *  prediction distribution.  It returns index of max value if predicted
   *  values are unique.  In the case of tie, the implementation solve it in
   *  psuedo-random way.
   *  @param preds an array of prediction distribution.  Length of arrays is equal to a number of classes+1.
   *  @return the best prediction (index of class, zero-based)
   */
  public static int getPrediction( float[] preds, double data[] ) {
    int best=1, tieCnt=0;   // Best class; count of ties
    for( int c=2; c<preds.length; c++) {
      if( preds[best] < preds[c] ) {
        best = c;               // take the max index
        tieCnt=0;               // No ties
      } else if (preds[best] == preds[c]) {
        tieCnt++;               // Ties
      }
    }
    if( tieCnt==0 ) return best-1; // Return zero-based best class
    // Tie-breaking logic
    float res = preds[best];    // One of the tied best results
    long hash = 0;              // hash for tie-breaking
    if( data != null )
      for( double d : data ) hash ^= Double.doubleToRawLongBits(d) >> 6; // drop 6 least significants bits of mantisa (layout of long is: 1b sign, 11b exp, 52b mantisa)
    int idx = (int)hash%(tieCnt+1);  // Which of the ties we'd like to keep
    for( best=1; best<preds.length; best++)
      if( res == preds[best] && --idx < 0 )
        return best-1;          // Return best
    throw H2O.fail();           // Should Not Reach Here
  }

  public static int getPrediction(float[] preds, int row) {
    int best=1, tieCnt=0;   // Best class; count of ties
    for( int c=2; c<preds.length; c++) {
      if( preds[best] < preds[c] ) {
        best = c;               // take the max index
        tieCnt=0;               // No ties
      } else if (preds[best] == preds[c]) {
        tieCnt++;               // Ties
      }
    }
    if( tieCnt==0 ) return best-1; // Return zero-based best class
    // Tie-breaking logic
    float res = preds[best];    // One of the tied best results
    int idx = row%(tieCnt+1);   // Which of the ties we'd like to keep
    for( best=1; best<preds.length; best++)
      if( res == preds[best] && --idx < 0 )
        return best-1;          // Return best
    throw H2O.fail();           // Should Not Reach Here
  }


  /**
   * Sample out-of-bag rows with given rate with help of given sampler.
   * It returns array of sampled rows. The first element of array contains a number
   * of sampled rows. The returned array can be larger than number of returned sampled
   * elements.
   *
   * @param nrows number of rows to sample from.
   * @param rate sampling rate
   * @param sampler random "dice"
   * @return an array contains numbers of sampled rows. The first element holds a number of sampled rows. The array length
   * can be greater than number of sampled rows.
   */
  public static int[] sampleOOBRows(int nrows, float rate, Random sampler) {
    return sampleOOBRows(nrows, rate, sampler, new int[1+(int)((1f-rate)*nrows*1.2f)]);
  }
  /**
   * In-situ version of {@link #sampleOOBRows(int, float, Random)}.
   *
   * @param oob an initial array to hold sampled rows. Can be internally realocted.
   * @return an array containing sampled rows.
   *
   * @see #sampleOOBRows(int, float, Random)
   */
  public static int[] sampleOOBRows(int nrows, float rate, Random sampler, int[] oob) {
    int oobcnt = 0; // Number of oob rows
    Arrays.fill(oob, 0);
    for(int row = 0; row < nrows; row++) {
      if (sampler.nextFloat() >= rate) { // it is out-of-bag row
        oob[1+oobcnt++] = row;
        if (1+oobcnt>=oob.length) oob = Arrays.copyOf(oob, (int)(1.2f*oob.length)+1);
      }
    }
    oob[0] = oobcnt;
    return oob;
  }
}
