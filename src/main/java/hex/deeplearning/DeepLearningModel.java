package hex.deeplearning;

import hex.FrameTask.DataInfo;
import hex.VarImp;
import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.D3Plot;
import water.util.Log;
import water.util.ModelUtils;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;

import static java.lang.Double.isNaN;

public class DeepLearningModel extends Model {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="Model info", json = true)
  private volatile DeepLearningModelInfo model_info;
  void set_model_info(DeepLearningModelInfo mi) { model_info = mi; }
  final public DeepLearningModelInfo model_info() { return model_info; }

  @API(help="Job that built the model", json = true)
  public Key jobKey;

  @API(help="Time to build the model", json = true)
  private long run_time;
  final private long start_time;

  @API(help="Number of training epochs", json = true)
  public double epoch_counter;

  @API(help="Number of rows in training data", json = true)
  public long training_rows;

  @API(help = "Scoring during model building")
  private Errors[] errors;

  public Errors last_scored() { return errors[errors.length-1]; }

  @Override public void delete() {
    super.delete();
    model_info.delete();
  }

  public static class Errors extends Iced {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "How many epochs the algorithm has processed")
    public double epoch_counter;
    @API(help = "How many rows the algorithm has processed")
    public long training_samples;
    @API(help = "How long the algorithm ran in ms")
    public long training_time_ms;

    //training/validation sets
    @API(help = "Whether a validation set was provided")
    boolean validation;
    @API(help = "Number of training set samples for scoring")
    public long score_training_samples;
    @API(help = "Number of validation set samples for scoring")
    public long score_validation_samples;

    @API(help="Do classification or regression")
    public boolean classification;

    // classification
    @API(help = "Confusion matrix on training data")
    public water.api.ConfusionMatrix train_confusion_matrix;
    @API(help = "Confusion matrix on validation data")
    public water.api.ConfusionMatrix valid_confusion_matrix;
    @API(help = "Classification error on training data")
    public double train_err = 1;
    @API(help = "Classification error on validation data")
    public double valid_err = 1;
    @API(help = "AUC on training data")
    public AUC trainAUC;
    @API(help = "AUC on validation data")
    public AUC validAUC;

    // regression
    @API(help = "Training MSE")
    public double train_mse = Double.POSITIVE_INFINITY;
    @API(help = "Validation MSE")
    public double valid_mse = Double.POSITIVE_INFINITY;
//    @API(help = "Training MCE")
//    public double train_mce = Double.POSITIVE_INFINITY;
//    @API(help = "Validation MCE")
//    public double valid_mce = Double.POSITIVE_INFINITY;

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      if (classification) {
        sb.append("Error on training data (misclassification)"
                + (trainAUC != null ? " [using threshold for " + trainAUC.threshold_criterion.toString().replace("_"," ") +"]: ": ": ")
                + String.format("%.2f", 100*train_err) + "%");

        if (trainAUC != null) sb.append(", AUC on training data: " + String.format("%.4f", 100*trainAUC.AUC) + "%");
        if (validation) sb.append("\nError on validation data (misclassification)"
                + (validAUC != null ? " [using threshold for " + validAUC.threshold_criterion.toString().replace("_"," ") +"]: ": ": ")
                + String.format("%.2f", (100 * valid_err)) + "%");
        if (validAUC != null) sb.append(", AUC on validation data: " + String.format("%.4f", 100*validAUC.AUC) + "%");
      } else {
        sb.append("Error on training data (MSE): " + train_mse);
        if (validation) sb.append("\nError on validation data (MSE): " + valid_mse);
      }
      return sb.toString();
    }
  }

  final private static class ConfMat extends hex.ConfusionMatrix {
    final private double _err;
    final private double _f1;
    public ConfMat(double err, double f1) {
      super(null);
      _err=err;
      _f1=f1;
    }
    @Override public double err() { return _err; }
    @Override public double F1() { return _f1; }
    @Override public double[] classErr() { return null; }
  }

  /** for grid search error reporting */
  @Override
  public hex.ConfusionMatrix cm() {
    final Errors lasterror = last_scored();
    if (errors == null) return null;
    water.api.ConfusionMatrix cm = lasterror.validation ?
            lasterror.valid_confusion_matrix :
            lasterror.train_confusion_matrix;
    if (cm == null || cm.cm == null) {
      if (lasterror.validation) {
        return new ConfMat(lasterror.valid_err, lasterror.validAUC != null ? lasterror.validAUC.F1() : 0);
      } else {
        return new ConfMat(lasterror.train_err, lasterror.trainAUC != null ? lasterror.trainAUC.F1() : 0);
      }
    }
    return new hex.ConfusionMatrix(cm.cm);
  }

  @Override
  public double mse() {
    if (errors == null) return super.mse();
    return last_scored().validation ? last_scored().valid_mse : last_scored().train_mse;
  }

  // This describes the model, together with the parameters
  // This will be shared: one per node
  public static class DeepLearningModelInfo extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help="Input data info")
    final private DataInfo data_info;
    public DataInfo data_info() { return data_info; }

    // model is described by parameters and the following 2 arrays
    final private float[][] weights; //one 2D weight matrix per layer (stored as a 1D array each)
    final private double[][] biases; //one 1D bias array per layer

    // helpers for storing previous step deltas
    // Note: These two arrays *could* be made transient and then initialized freshly in makeNeurons() and in DeepLearningTask.initLocal()
    // But then, after each reduction, the weights would be lost and would have to restart afresh -> not *exactly* right, but close...
    private float[][] weights_momenta;
    private double[][] biases_momenta;


    // helpers for AdaDelta
    private float[][] E_dx2;
    private float[][] E_g2;

    // compute model size (number of model parameters required for making predictions)
    // momenta are not counted here, but they are needed for model building
    public long size() {
      long siz = 0;
      for (float[] w : weights) siz += w.length;
      for (double[] b : biases) siz += b.length;
      return siz;
    }

    // accessors to (shared) weights and biases - those will be updated racily (c.f. Hogwild!)
    boolean has_momenta() { return parameters.momentum_start != 0 || parameters.momentum_stable != 0; }
    boolean adaDelta() { return parameters.rho > 0 && parameters.epsilon > 0; }
    public final float[] get_weights(int i) { return weights[i]; }
    public final double[] get_biases(int i) { return biases[i]; }
    public final float[] get_weights_momenta(int i) { return weights_momenta[i]; }
    public final double[] get_biases_momenta(int i) { return biases_momenta[i]; }
    public final float[] get_E_dx2(int i) { return E_dx2[i]; }
    public final float[] get_E_g2(int i) { return E_g2[i]; }

    @API(help = "Model parameters", json = true)
    final private DeepLearning parameters;
    public final DeepLearning get_params() { return parameters; }
    public final DeepLearning job() { return get_params(); }

    @API(help = "Mean rate", json = true)
    private double[] mean_rate;

    @API(help = "RMS rate", json = true)
    private double[] rms_rate;

    @API(help = "Mean bias", json = true)
    private double[] mean_bias;

    @API(help = "RMS bias", json = true)
    private double[] rms_bias;

    @API(help = "Mean weight", json = true)
    private double[] mean_weight;

    @API(help = "RMS weight", json = true)
    public double[] rms_weight;

    @API(help = "Unstable", json = true)
    private volatile boolean unstable = false;
    public boolean unstable() { return unstable; }
    public void set_unstable() { unstable = true; computeStats(); }

    @API(help = "Processed samples", json = true)
    private long processed_global;
    public synchronized long get_processed_global() { return processed_global; }
    public synchronized void set_processed_global(long p) { processed_global = p; }
    public synchronized void add_processed_global(long p) { processed_global += p; }

    private long processed_local;
    public synchronized long get_processed_local() { return processed_local; }
    public synchronized void set_processed_local(long p) { processed_local = p; }
    public synchronized void add_processed_local(long p) { processed_local += p; }

    public synchronized long get_processed_total() { return processed_global + processed_local; }

    // package local helpers
    final int[] units; //number of neurons per layer, extracted from parameters and from datainfo

    public DeepLearningModelInfo(final DeepLearning params, final DataInfo dinfo) {
      data_info = dinfo;
      final int num_input = dinfo.fullN();
      final int num_output = params.classification ? dinfo._adaptedFrame.lastVec().domain().length : 1;
      assert(num_input > 0);
      assert(num_output > 0);
      parameters = params;
      if (has_momenta() && adaDelta()) throw new IllegalArgumentException("Cannot have non-zero momentum and non-zero AdaDelta parameters at the same time.");
      final int layers=parameters.hidden.length;
      // units (# neurons for each layer)
      units = new int[layers+2];
      units[0] = num_input;
      System.arraycopy(parameters.hidden, 0, units, 1, layers);
      units[layers+1] = num_output;
      // weights (to connect layers)
      weights = new float[layers+1][];
      for (int i=0; i<=layers; ++i) weights[i] = new float[units[i]*units[i+1]];
      // biases (only for hidden layers and output layer)
      biases = new double[layers+1][];
      for (int i=0; i<=layers; ++i) biases[i] = new double[units[i+1]];
      fillHelpers();
      // for diagnostics
      mean_rate = new double[units.length];
      rms_rate = new double[units.length];
      mean_bias = new double[units.length];
      rms_bias = new double[units.length];
      mean_weight = new double[units.length];
      rms_weight = new double[units.length];
    }

    void fillHelpers() {
      if (has_momenta()) {
        if (weights_momenta != null) return;
        weights_momenta = new float[weights.length][];
        for (int i=0; i<weights_momenta.length; ++i) weights_momenta[i] = new float[units[i]*units[i+1]];
        biases_momenta = new double[biases.length][];
        for (int i=0; i<biases_momenta.length; ++i) biases_momenta[i] = new double[units[i+1]];
      }
      else {
        //AdaGrad
        if (E_dx2 != null) return;
        E_dx2 = new float[weights.length][];
        for (int i=0; i<E_dx2.length; ++i) E_dx2[i] = new float[units[i]*units[i+1]];
        E_g2 = new float[weights.length][];
        for (int i=0; i<E_g2.length; ++i) E_g2[i] = new float[units[i]*units[i+1]];
      }
    }
    public void delete() {
      // ugly: whoever made data_info should also clean this up... but sometimes it was made by Weaver from UKV!
      if (data_info()._adaptedFrame.lastVec()._key!=null) UKV.remove(data_info()._adaptedFrame.lastVec()._key);
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      if (parameters.diagnostics) {
        computeStats();
        if (!parameters.quiet_mode) {
          Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(this);
          sb.append("Status of Neuron Layers:\n");
          sb.append("#  Units         Type      Dropout    L1       L2    " + (parameters.adaptive_rate ? "  Rate (Mean,RMS)   " : "  Rate      Momentum") + "   Weight (Mean, RMS)      Bias (Mean,RMS)\n");
          final String format = "%7g";
          for (int i=0; i<neurons.length; ++i) {
            sb.append((i+1) + " " + String.format("%6d", neurons[i].units)
                    + " " + String.format("%16s", neurons[i].getClass().getSimpleName()));
            if (i == 0) {
              sb.append("  " + String.format("%.5g", neurons[i].params.input_dropout_ratio*100) + "%\n");
              continue;
            }
            else if (i < neurons.length-1) {
              sb.append( neurons[i] instanceof Neurons.TanhDropout
                      || neurons[i] instanceof Neurons.RectifierDropout
                      || neurons[i] instanceof Neurons.MaxoutDropout ? "    50%   " : "     0%   ");
            } else {
              sb.append("          ");
            }
            sb.append(
                    " " + String.format("%5f", neurons[i].params.l1)
                            + " " + String.format("%5f", neurons[i].params.l2)
                            + " " + (parameters.adaptive_rate ? (" (" + String.format(format, mean_rate[i]) + ", " + String.format(format, rms_rate[i]) + ")" )
                                    : (String.format("%10g", neurons[i].rate(get_processed_total())) + " " + String.format("%5f", neurons[i].momentum(get_processed_total()))))
                            + " (" + String.format(format, mean_weight[i])
                            + ", " + String.format(format, rms_weight[i]) + ")"
                            + " (" + String.format(format, mean_bias[i])
                            + ", " + String.format(format, rms_bias[i]) + ")\n");
          }
        }
      }
      return sb.toString();
    }

    // DEBUGGING
    public String toStringAll() {
      StringBuilder sb = new StringBuilder();
      sb.append(toString());
      for (int i=0; i<weights.length; ++i)
        sb.append("\nweights["+i+"][]="+Arrays.toString(weights[i]));
      for (int i=0; i<biases.length; ++i)
        sb.append("\nbiases["+i+"][]="+Arrays.toString(biases[i]));
      if (weights_momenta != null) {
        for (int i=0; i<weights_momenta.length; ++i)
          sb.append("\nweights_momenta["+i+"][]="+Arrays.toString(weights_momenta[i]));
      }
      if (biases_momenta != null) {
        for (int i=0; i<biases_momenta.length; ++i)
          sb.append("\nbiases_momenta["+i+"][]="+Arrays.toString(biases_momenta[i]));
      }
      sb.append("\nunits[]="+Arrays.toString(units));
      sb.append("\nprocessed global: "+get_processed_global());
      sb.append("\nprocessed local:  "+get_processed_local());
      sb.append("\nprocessed total:  " + get_processed_total());
      sb.append("\n");
      return sb.toString();
    }

    void initializeMembers() {
      randomizeWeights();
      //TODO: determine good/optimal/best initialization scheme for biases
      // hidden layers
      for (int i=0; i<parameters.hidden.length; ++i) {
        if (parameters.activation == DeepLearning.Activation.Rectifier
                || parameters.activation == DeepLearning.Activation.RectifierWithDropout
                || parameters.activation == DeepLearning.Activation.Maxout
                || parameters.activation == DeepLearning.Activation.MaxoutWithDropout
                ) {
//          Arrays.fill(biases[i], 1.); //old behavior
          Arrays.fill(biases[i], i == 0 ? 0.5 : 1.); //new behavior, might be slightly better
        }
        else if (parameters.activation == DeepLearning.Activation.Tanh || parameters.activation == DeepLearning.Activation.TanhWithDropout) {
          Arrays.fill(biases[i], 0.0);
        }
      }
      Arrays.fill(biases[biases.length-1], 0.0); //output layer
    }
    public void add(DeepLearningModelInfo other) {
      Utils.add(weights, other.weights);
      Utils.add(biases,  other.biases);
      if (has_momenta()) {
        assert(other.has_momenta());
        Utils.add(weights_momenta, other.weights_momenta);
        Utils.add(biases_momenta,  other.biases_momenta);
      }
      if (adaDelta()) {
        assert(other.adaDelta());
        Utils.add(E_dx2, other.E_dx2);
        Utils.add(E_g2,  other.E_g2);
      }
      add_processed_local(other.get_processed_local());
    }
    protected void div(double N) {
      for (float[] weight : weights) Utils.div(weight, (float) N);
      for (double[] bias : biases) Utils.div(bias, N);
      if (has_momenta()) {
        for (float[] weight_momenta : weights_momenta) Utils.div(weight_momenta, (float) N);
        for (double[] bias_momenta : biases_momenta) Utils.div(bias_momenta, N);
      }
      if (adaDelta()) {
        for (float[] dx2 : E_dx2) Utils.div(dx2, (float) N);
        for (float[] g2 : E_g2) Utils.div(g2, (float) N);
      }
    }
    double uniformDist(Random rand, double min, double max) {
      return min + rand.nextFloat() * (max - min);
    }
    void randomizeWeights() {
      for (int i=0; i<weights.length; ++i) {
        final Random rng = water.util.Utils.getDeterRNG(get_params().seed + 0xBAD5EED + i+1); //to match NeuralNet behavior
        for( int j = 0; j < weights[i].length; j++ ) {
          if (parameters.initial_weight_distribution == DeepLearning.InitialWeightDistribution.UniformAdaptive) {
            // cf. http://machinelearning.wustl.edu/mlpapers/paper_files/AISTATS2010_GlorotB10.pdf
            final double range = Math.sqrt(6. / (units[i] + units[i+1]));
            weights[i][j] = (float)uniformDist(rng, -range, range);
            if (i==weights.length-1 && parameters.classification) weights[i][j] *= 4; //Softmax might need an extra factor 4, since it's like a sigmoid
          }
          else if (parameters.initial_weight_distribution == DeepLearning.InitialWeightDistribution.Uniform) {
            weights[i][j] = (float)uniformDist(rng, -parameters.initial_weight_scale, parameters.initial_weight_scale);
          }
          else if (parameters.initial_weight_distribution == DeepLearning.InitialWeightDistribution.Normal) {
            weights[i][j] = (float)(rng.nextGaussian() * parameters.initial_weight_scale);
          }
        }
      }
    }

    // TODO: Add "subset randomize" function
//        int count = Math.min(15, _previous.units);
//        double min = -.1f, max = +.1f;
//        //double min = -1f, max = +1f;
//        for( int o = 0; o < units; o++ ) {
//          for( int n = 0; n < count; n++ ) {
//            int i = rand.nextInt(_previous.units);
//            int w = o * _previous.units + i;
//            _w[w] = uniformDist(rand, min, max);
//          }
//        }

    /**
     * Compute Variable Importance, based on Garson (but using absolute values of the weights)
     * @return variable importances for input features
     */
    public float[] computeVariableImportances() {
      float[] vi = new float[units[0]];
      Arrays.fill(vi, 0f);

      float[][] Qik = new float[units[0]][units[2]]; //importance of input i on output k
      float[] sum_wj = new float[units[1]]; //sum of incoming weights into first hidden layer
      for (float[] Qi : Qik) Arrays.fill(Qi, 0f);
      Arrays.fill(sum_wj, 0f);

      // compute sum of absolute incoming weights
      for( int j = 0; j < units[1]; j++ ) {
        for( int i = 0; i < units[0]; i++ ) {
          float wij = weights[0][j*units[0]+i];
          sum_wj[j] += Math.abs(wij);
        }
      }
      // compute importance of input i on output k as product of connecting weights going through j
      for( int i = 0; i < units[0]; i++ ) {
        for( int k = 0; k < units[2]; k++ ) {
          for( int j = 0; j < units[1]; j++ ) {
            float wij = weights[0][j*units[0]+i];
            float wjk = weights[1][k*units[1]+j];
            Qik[i][k] += Math.abs(wij/sum_wj[j] * wjk);
          }
        }
      }
      // normalize Qik over all outputs k
      for( int k = 0; k < units[2]; k++ ) {
        float sumQk = 0;
        for( int i = 0; i < units[0]; i++ ) sumQk += Qik[i][k];
        for( int i = 0; i < units[0]; i++ ) Qik[i][k] /= sumQk;
      }
      // importance for feature i is the sum over k of i->k importances
      for( int i = 0; i < units[0]; i++ ) vi[i] = Utils.sum(Qik[i]);

      //normalize importances such that sum(vi) = 1
      Utils.div(vi, Utils.sum(vi));
      return vi;
    }

    // compute stats on all nodes
    public void computeStats() {
      double[][] rate = parameters.adaptive_rate ? new double[units.length-1][] : null;
      for( int y = 1; y < units.length; y++ ) {
        mean_rate[y] = rms_rate[y] = 0;
        mean_bias[y] = rms_bias[y] = 0;
        mean_weight[y] = rms_weight[y] = 0;
        for(int u = 0; u < biases[y-1].length; u++) {
          mean_bias[y] += biases[y-1][u];
        }
        if (rate != null) rate[y-1] = new double[weights[y-1].length];
        for(int u = 0; u < weights[y-1].length; u++) {
          mean_weight[y] += weights[y-1][u];
          if (rate != null) {
            final double RMS_dx = Math.sqrt(E_dx2[y-1][u]+parameters.epsilon);
            final double RMS_g = Math.sqrt(E_g2[y-1][u]+parameters.epsilon);
            rate[y-1][u] = (RMS_dx/RMS_g); //not exactly right, RMS_dx should be from the previous time step -> but close enough for diagnostics.
            mean_rate[y] += rate[y-1][u];
          }
        }
        mean_bias[y] /= biases[y-1].length;
        mean_weight[y] /= weights[y-1].length;
        if (rate != null) mean_rate[y] /= rate[y-1].length;

        for(int u = 0; u < biases[y-1].length; u++) {
          final double db = biases[y-1][u] - mean_bias[y];
          rms_bias[y] += db * db;
        }
        for(int u = 0; u < weights[y-1].length; u++) {
          final double dw = weights[y-1][u] - mean_weight[y];
          rms_weight[y] += dw * dw;
          if (rate != null) {
            final double drate = rate[y-1][u] - mean_rate[y];
            rms_rate[y] += drate * drate;
          }
        }
        rms_bias[y] = Math.sqrt(rms_bias[y]/biases[y-1].length);
        rms_weight[y] = Math.sqrt(rms_weight[y]/weights[y-1].length);
        if (rate != null) rms_rate[y] = Math.sqrt(rms_rate[y]/rate[y-1].length);

        unstable |= isNaN(mean_bias[y])  || isNaN(rms_bias[y])
                || isNaN(mean_weight[y]) || isNaN(rms_weight[y]);

        // Abort the run if weights or biases are unreasonably large (Note that all input values are normalized upfront)
        // This can happen with Rectifier units when L1/L2/max_w2 are all set to 0, especially when using more than 1 hidden layer.
        final double thresh = 1e10;
        unstable |= mean_bias[y] > thresh  || isNaN(mean_bias[y])
                || rms_bias[y] > thresh    || isNaN(rms_bias[y])
                || mean_weight[y] > thresh || isNaN(mean_weight[y])
                || rms_weight[y] > thresh  || isNaN(rms_weight[y]);
      }
    }
  }

  /**
   * Constructor to restart from a checkpointed model
   * @param cp Checkpoint to restart from
   * @param selfKey New destination key for the model
   * @param jobKey New job key (job which updates the model)
   */
  public DeepLearningModel(DeepLearningModel cp, Key selfKey, Key jobKey) {
    super(selfKey, cp._dataKey, cp.model_info().data_info()._adaptedFrame, cp._priorClassDist);
    this.jobKey = jobKey;
    model_info = (DeepLearningModelInfo)cp.model_info.clone();
    model_info.parameters.destination_key = selfKey;
    model_info.parameters.job_key = jobKey;
    start_time = cp.start_time;
    run_time = cp.run_time;
    errors = cp.errors.clone();
    training_rows = cp.training_rows; //copy the value to display the right number on the model page before training has started
    model_info.parameters.start_time = System.currentTimeMillis(); //for displaying the model progress
    _timeLastScoreEnter = System.currentTimeMillis();
    _timeLastScoreStart = 0;
    _timeLastScoreEnd = 0;
    _timeLastPrintStart = 0;
  }

  public DeepLearningModel(Key selfKey, Key jobKey, Key dataKey, DataInfo dinfo, DeepLearning params, float[] priorDist) {
    super(selfKey, dataKey, dinfo._adaptedFrame, priorDist);
    this.jobKey = jobKey;
    run_time = 0;
    start_time = System.currentTimeMillis();
    _timeLastScoreEnter = start_time;
    model_info = new DeepLearningModelInfo(params, dinfo);
    errors = new Errors[1];
    errors[0] = new Errors();
    errors[0].validation = (params.validation != null);
  }

  private long _timeLastScoreEnter; //not transient: needed for HTML display page
  transient private long _timeLastScoreStart;
  transient private long _timeLastScoreEnd;
  transient private long _timeLastPrintStart;
  /**
   *
   * @param train training data from which the model is built (for epoch counting only)
   * @param ftrain potentially downsampled training data for scoring
   * @param ftest  potentially downsampled validation data for scoring
   * @param timeStart start time in milliseconds, used to report training speed
   * @param job_key key of the owning job
   * @return true if model building is ongoing
   */
  boolean doScoring(Frame train, Frame ftrain, Frame ftest, long timeStart, Key job_key) {
    final long now = System.currentTimeMillis();
    epoch_counter = (float)model_info().get_processed_total()/train.numRows();
    run_time += now-_timeLastScoreEnter;
    _timeLastScoreEnter = now;
    boolean keep_running = (epoch_counter < model_info().get_params().epochs);
    final long sinceLastScore = now -_timeLastScoreStart;
    final long sinceLastPrint = now -_timeLastPrintStart;
    final long samples = model_info().get_processed_total();
    if (!keep_running || sinceLastPrint > model_info().get_params().score_interval*1000) {
      _timeLastPrintStart = now;
      Log.info("Training time: " + PrettyPrint.msecs(run_time, true)
              + ". Processed " + String.format("%,d", samples) + " samples" + " (" + String.format("%.3f", epoch_counter) + " epochs)."
              + " Speed: " + String.format("%.3f", 1000.*samples/run_time) + " samples/sec.");
    }
    // this is potentially slow - only do every so often
    if( !keep_running ||
            (sinceLastScore > model_info().get_params().score_interval*1000 //don't score too often
        &&(double)(_timeLastScoreEnd-_timeLastScoreStart)/sinceLastScore < model_info().get_params().score_duty_cycle) ) { //duty cycle
      final boolean printme = !model_info().get_params().quiet_mode;
      if (printme) Log.info("Scoring the model.");
      _timeLastScoreStart = now;
      // compute errors
      Errors err = new Errors();
      err.classification = isClassifier();
      assert(err.classification == model_info().get_params().classification);
      err.training_time_ms = run_time;
      err.epoch_counter = epoch_counter;
      err.validation = ftest != null;
      err.training_samples = model_info().get_processed_total();
      err.score_training_samples = ftrain.numRows();
      err.train_confusion_matrix = new ConfusionMatrix();
      if (err.classification && nclasses()==2) err.trainAUC = new AUC();
      model_info().toString();
      final Frame trainPredict = score(ftrain, false);
      final double trainErr = calcError(ftrain, trainPredict, "training", printme, err.train_confusion_matrix, err.trainAUC);
      if (isClassifier()) err.train_err = trainErr;
      else err.train_mse = trainErr;

      trainPredict.delete();

      if (err.validation) {
        assert ftest != null;
        err.score_validation_samples = ftest.numRows();
        err.valid_confusion_matrix = new ConfusionMatrix();
        if (err.classification && nclasses()==2) err.validAUC = new AUC();
        Job.ValidatedJob.Response2CMAdaptor vadaptor = model_info().job().getValidAdaptor();
        Vec tmp = null;
        if (isClassifier() && vadaptor.needsAdaptation2CM()) tmp = ftest.remove(ftest.vecs().length-1);
        final Frame validPredict = score(ftest, false);
        // Adapt output response domain, in case validation domain is different from training domain
        // Note: doesn't change predictions, just the *possible* label domain
        if (isClassifier() && vadaptor.needsAdaptation2CM()) {
          ftest.add("adaptedValidationResponse", tmp);
          final Vec CMadapted = vadaptor.adaptModelResponse2CM(validPredict.vecs()[0]);
          validPredict.replace(0, CMadapted); //replace label
          validPredict.add("to_be_deleted", CMadapted); //keep the Vec around to be deleted later (no leak)
        }
        final double validErr = calcError(ftest, validPredict, "validation", printme, err.valid_confusion_matrix, err.validAUC);
        if (isClassifier()) err.valid_err = validErr;
        else err.valid_mse = validErr;
        validPredict.delete();
      }

      // keep output JSON small
      if (errors.length > 1) {
        if (last_scored().trainAUC != null) last_scored().trainAUC.clear();
        if (last_scored().validAUC != null) last_scored().validAUC.clear();
      }

      // only keep confusion matrices for the last step if there are fewer than specified number of output classes
      if (err.train_confusion_matrix.cm != null
              && err.train_confusion_matrix.cm.length >= model_info().get_params().max_confusion_matrix_size) {
        err.train_confusion_matrix = null;
        err.valid_confusion_matrix = null;
      }

      // enlarge the error array by one, push latest score back
      if (errors == null) {
         errors = new Errors[]{err};
      } else {
        Errors[] err2 = new Errors[errors.length+1];
        System.arraycopy(errors, 0, err2, 0, errors.length);
        err2[err2.length-1] = err;
        errors = err2;
      }
      _timeLastScoreEnd = System.currentTimeMillis();
      // print the freshly scored model to ASCII
      for (String s : toString().split("\n")) Log.info(s);
      if (printme) Log.info("Scoring time: " + PrettyPrint.msecs(System.currentTimeMillis() - now, true));
    }
    if (model_info().unstable()) {
      Log.err("Canceling job since the model is unstable (exponential growth observed).");
      Log.err("Try a bounded activation function or regularization with L1, L2 or max_w2 and/or use a smaller learning rate or faster annealing.");
      keep_running = false;
    } else if ( (isClassifier() && last_scored().train_err <= model_info().get_params().classification_stop)
        || (!isClassifier() && last_scored().train_mse <= model_info().get_params().regression_stop) ) {
      Log.info("Achieved requested predictive accuracy on the training data. Model building completed.");
      keep_running = false;
    }
    update(job_key);
//    System.out.println(this);
    return keep_running;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(model_info.toString());
    sb.append(last_scored().toString());
    return sb.toString();
  }

  public String toStringAll() {
    StringBuilder sb = new StringBuilder();
    sb.append(model_info.toStringAll());
    sb.append(last_scored().toString());
    return sb.toString();
  }

  /**
   * Predict from raw double values representing
   * @param data raw array containing categorical values (horizontalized to 1,0,0,1,0,0 etc.) and numerical values (0.35,1.24,5.3234,etc), both can contain NaNs
   * @param preds predicted label and per-class probabilities (for classification), predicted target (regression), can contain NaNs
   * @return preds, can contain NaNs
   */
  @Override public float[] score0(double[] data, float[] preds) {
    Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(model_info);
    ((Neurons.Input)neurons[0]).setInput(-1, data);
    DeepLearningTask.step(-1, neurons, model_info, false, null);
    double[] out = neurons[neurons.length - 1]._a;
    if (isClassifier()) {
      assert(preds.length == out.length+1);
      for (int i=0; i<preds.length-1; ++i) {
        preds[i+1] = (float)out[i];
        if (Float.isNaN(preds[i+1])) throw new RuntimeException("Predicted class probability NaN!");
      }
      preds[0] = ModelUtils.getPrediction(preds, data);
    } else {
      assert(preds.length == 1 && out.length == 1);
      if (model_info().data_info()._normRespMul != null)
        preds[0] = (float)(out[0] / model_info().data_info()._normRespMul[0] + model_info().data_info()._normRespSub[0]);
      else
        preds[0] = (float)out[0];
      if (Float.isNaN(preds[0])) throw new RuntimeException("Predicted regression target NaN!");
    }
    return preds;
  }

  /**
   * Compute the model error for a given test data set
   * For multi-class classification, this is the classification error based on assigning labels for the highest predicted per-class probability.
   * For binary classification, this is the classification error based on assigning labels using the optimal threshold for maximizing the F1 score.
   * For regression, this is the mean squared error (MSE).
   * @param ftest Frame containing test data
   * @param fpreds Frame containing predicted data (classification: label + per-class probabilities, regression: target)
   * @param label Name for the scored data set
   * @param printCM Whether to print the confusion matrix to stdout
   * @param cm Confusion Matrix object to populate for multi-class classification (also used for regression)
   * @param auc AUC object to populate for binary classification
   * @return model error, see description above
   */
  public double calcError(Frame ftest, Frame fpreds, String label, boolean printCM, ConfusionMatrix cm, AUC auc) {
    StringBuilder sb = new StringBuilder();
    double error;

    // populate AUC
    if (auc != null) {
      auc.actual = ftest;
      auc.vactual = ftest.lastVec();
      auc.predict = fpreds;
      auc.vpredict = fpreds.vecs()[2]; //binary classifier (label, prob0, prob1 (THIS ONE), adaptedlabel)
      auc.threshold_criterion = AUC.ThresholdCriterion.maximum_F1;
      auc.serve();
      auc.toASCII(sb);
      error = auc.err(); //using optimal threshold for F1
    }
    // populate CM
    else {
      if (cm == null) cm = new ConfusionMatrix();
      cm.actual = ftest;
      cm.vactual = ftest.lastVec(); //original vector or adapted response (label) if CM adaptation was done
      cm.predict = fpreds;
      cm.vpredict = fpreds.vecs()[0]; //ditto
      cm.serve();
      cm.toASCII(sb);
      error = isClassifier() ? new hex.ConfusionMatrix(cm.cm).err() : cm.mse;
    }
    if (printCM && (auc != null || cm.cm==null /*regression*/ || cm.cm.length <= model_info().get_params().max_confusion_matrix_size)) {
      Log.info("Scoring on " + label + " data:");
      for (String s : sb.toString().split("\n")) Log.info(s);
    }
    return error;
  }

  public boolean generateHTML(String title, StringBuilder sb) {
    if (_key == null) {
      DocGen.HTML.title(sb, "No model yet");
      return true;
    }

    final String mse_format = "%g";
//    final String cross_entropy_format = "%2.6f";

    // stats for training and validation
    final Errors error = last_scored();

    DocGen.HTML.title(sb, title);

    model_info.job().toHTML(sb);
    Inspect2 is2 = new Inspect2();
    final Key val_key = model_info().parameters.validation != null ? model_info().parameters.validation._key : null;
    sb.append("<div class='alert'>Actions: "
            + (Job.isRunning(jobKey) ? "<i class=\"icon-stop\"></i>" + Cancel.link(jobKey, "Stop training") + ", " : "")
            + is2.link("Inspect training data (" + _dataKey + ")", _dataKey) + ", "
            + (val_key != null ? (is2.link("Inspect validation data (" + val_key + ")", val_key) + ", ") : "")
            + water.api.Predict.link(_key, "Score on dataset") + ", "
            + DeepLearning.link(_dataKey, "Compute new model", null, responseName(), val_key) + ", "
            + (Job.isEnded(jobKey) ? "<i class=\"icon-play\"></i>" + DeepLearning.link(_dataKey, "Continue training this model", _key, responseName(), val_key) : "")
            + "</div>");

    DocGen.HTML.paragraph(sb, "Model Key: " + _key);
    DocGen.HTML.paragraph(sb, "Job Key: " + jobKey);
    DocGen.HTML.paragraph(sb, "Model type: " + (model_info().parameters.classification ? " Classification" : " Regression") + ", predicting: " + responseName());
    DocGen.HTML.paragraph(sb, "Number of model parameters (weights/biases): " + String.format("%,d", model_info().size()));

    if (model_info.unstable()) {
      final String msg = "Job was aborted due to observed numerical instability (exponential growth)."
              + " Try a bounded activation function or regularization with L1, L2 or max_w2 and/or use a smaller learning rate or faster annealing.";
      DocGen.HTML.section(sb, "=======================================================================================");
      DocGen.HTML.section(sb, msg);
      DocGen.HTML.section(sb, "=======================================================================================");
    }

    DocGen.HTML.title(sb, "Progress");
    // update epoch counter every time the website is displayed
    epoch_counter = training_rows > 0 ? (float)model_info().get_processed_total()/training_rows : 0;
    final double progress = model_info.get_params().progress();

    if (model_info.parameters != null && model_info.parameters.diagnostics) {
      DocGen.HTML.section(sb, "Status of Neuron Layers");
      sb.append("<table class='table table-striped table-bordered table-condensed'>");
      sb.append("<tr>");
      sb.append("<th>").append("#").append("</th>");
      sb.append("<th>").append("Units").append("</th>");
      sb.append("<th>").append("Type").append("</th>");
      sb.append("<th>").append("Dropout").append("</th>");
      sb.append("<th>").append("L1").append("</th>");
      sb.append("<th>").append("L2").append("</th>");
      if (model_info.get_params().adaptive_rate) {
        sb.append("<th>").append("Rate (Mean, RMS)").append("</th>");
      } else {
        sb.append("<th>").append("Rate").append("</th>");
        sb.append("<th>").append("Momentum").append("</th>");
      }
      sb.append("<th>").append("Weight (Mean, RMS)").append("</th>");
      sb.append("<th>").append("Bias (Mean, RMS)").append("</th>");
      sb.append("</tr>");
      Neurons[] neurons = DeepLearningTask.makeNeuronsForTesting(model_info()); //link the weights to the neurons, for easy access
      for (int i=0; i<neurons.length; ++i) {
        sb.append("<tr>");
        sb.append("<td>").append("<b>").append(i+1).append("</b>").append("</td>");
        sb.append("<td>").append("<b>").append(neurons[i].units).append("</b>").append("</td>");
        sb.append("<td>").append(neurons[i].getClass().getSimpleName()).append("</td>");

        if (i == 0) {
          sb.append("<td>");
          sb.append(formatPct(neurons[i].params.input_dropout_ratio));
          sb.append("</td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          if (!model_info.get_params().adaptive_rate) sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("<td></td>");
          sb.append("</tr>");
          continue;
        }
        else if (i < neurons.length-1) {
          sb.append("<td>");
          sb.append( neurons[i] instanceof Neurons.TanhDropout
                  || neurons[i] instanceof Neurons.RectifierDropout
                  || neurons[i] instanceof Neurons.MaxoutDropout ? "50%" : "0%");
          sb.append("</td>");
        } else {
          sb.append("<td></td>");
        }

        final String format = "%g";
        sb.append("<td>").append(neurons[i].params.l1).append("</td>");
        sb.append("<td>").append(neurons[i].params.l2).append("</td>");
        if (model_info.get_params().adaptive_rate) {
          sb.append("<td>(").append(String.format(format, model_info.mean_rate[i])).
                  append(", ").append(String.format(format, model_info.rms_rate[i])).append(")</td>");
        } else {
          sb.append("<td>").append(String.format("%.5g", neurons[i].rate(error.training_samples))).append("</td>");
          sb.append("<td>").append(String.format("%.5f", neurons[i].momentum(error.training_samples))).append("</td>");
        }
        sb.append("<td>(").append(String.format(format, model_info.mean_weight[i])).
                append(", ").append(String.format(format, model_info.rms_weight[i])).append(")</td>");
        sb.append("<td>(").append(String.format(format, model_info.mean_bias[i])).
                append(", ").append(String.format(format, model_info.rms_bias[i])).append(")</td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
    }

    if (isClassifier()) {
      DocGen.HTML.section(sb, "Classification error on training data: " + formatPct(error.train_err));
//      DocGen.HTML.section(sb, "Training cross entropy: " + String.format(cross_entropy_format, error.train_mce));
      if(error.validation) {
        DocGen.HTML.section(sb, "Classification error on validation data: " + formatPct(error.valid_err));
//        DocGen.HTML.section(sb, "Validation mean cross entropy: " + String.format(cross_entropy_format, error.valid_mce));
      }
    } else {
      DocGen.HTML.section(sb, "MSE on training data: " + String.format(mse_format, error.train_mse));
      if(error.validation) {
        DocGen.HTML.section(sb, "MSE on validation data: " + String.format(mse_format, error.valid_mse));
      }
    }
    DocGen.HTML.paragraph(sb, "Epochs: " + String.format("%.3f", epoch_counter) + " / " + String.format("%.3f", model_info.parameters.epochs));
    final boolean isEnded = Job.isEnded(model_info().job().self());
    final long time_so_far = isEnded ? run_time : run_time + System.currentTimeMillis() - _timeLastScoreEnter;
    if (time_so_far > 0) {
      DocGen.HTML.paragraph(sb, "Training speed: " + String.format("%,d", model_info().get_processed_total() * 1000 / time_so_far) + " samples/s");
    }
    DocGen.HTML.paragraph(sb, "Training time: " + PrettyPrint.msecs(time_so_far, true));
    if (progress > 0 && !isEnded)
      DocGen.HTML.paragraph(sb, "Estimated time left: " +PrettyPrint.msecs((long)(time_so_far*(1-progress)/progress), true));

    long score_train = error.score_training_samples;
    long score_valid = error.score_validation_samples;
    final boolean fulltrain = score_train==0 || score_train == model_info().data_info()._adaptedFrame.numRows();
    final boolean fullvalid = score_valid==0 || score_valid == model_info().get_params().validation.numRows();

    final String toolarge = " Not shown here - too large: number of classes (" + model_info.units[model_info.units.length-1]
            + ") is greater than the specified limit of " + model_info().get_params().max_confusion_matrix_size + ".";
    boolean smallenough = model_info.units[model_info.units.length-1] <= model_info().get_params().max_confusion_matrix_size;

    if (isClassifier()) {
      // print AUC
      if (error.validAUC != null) {
        error.validAUC.toHTML(sb);
      }
      else if (error.trainAUC != null) {
        error.trainAUC.toHTML(sb);
      }
      else {
        if (error.validation) {
          String cmTitle = "Confusion matrix reported on validation data" + (fullvalid ? "" : " (" + score_valid + " samples)") + ":";
          sb.append("<h5>" + cmTitle);
          if (error.valid_confusion_matrix != null && smallenough) {
            sb.append("</h5>");
            error.valid_confusion_matrix.toHTML(sb);
          } else if (smallenough) sb.append(" Not yet computed.</h5>");
          else sb.append(" Too large." + "</h5>");
        } else {
          String cmTitle = "Confusion matrix reported on training data" + (fulltrain ? "" : " (" + score_train + " samples)") + ":";
          sb.append("<h5>" + cmTitle);
          if (error.train_confusion_matrix != null && smallenough) {
            sb.append("</h5>");
            error.train_confusion_matrix.toHTML(sb);
          } else if (smallenough) sb.append(" Not yet computed.</h5>");
          else sb.append(toolarge + "</h5>");
        }
      }
    }

    // Variable importance
    if (model_info().get_params().variable_importances) {
      final float [] varimp = model_info().computeVariableImportances();
      new VarImp(varimp, Arrays.copyOfRange(model_info().data_info().coefNames(), 0, varimp.length)).toHTML(sb);
    }

    DocGen.HTML.title(sb, "Scoring history");
    if (errors.length > 1) {
      // training
      {
        final long pts = fulltrain ? model_info().data_info()._adaptedFrame.numRows() : score_train;
        String training = "Number of training data samples for scoring: " + (fulltrain ? "all " : "") + pts;
        if (pts < 1000 && model_info().data_info()._adaptedFrame.numRows() >= 1000) training += " (low, scoring might be inaccurate -> consider increasing this number in the expert mode)";
        if (pts > 100000) training += " (large, scoring can be slow -> consider reducing this number in the expert mode or scoring manually)";
        DocGen.HTML.paragraph(sb, training);
      }
      // validation
      if (error.validation) {
        final long ptsv = fullvalid ? model_info().get_params().validation.numRows() : score_valid;
        String validation = "Number of validation data samples for scoring: " + (fullvalid ? "all " : "") + ptsv;
        if (ptsv < 1000 && model_info().get_params().validation.numRows() >= 1000) validation += " (low, scoring might be inaccurate -> consider increasing this number in the expert mode)";
        if (ptsv > 100000) validation += " (large, scoring can be slow -> consider reducing this number in the expert mode or scoring manually)";
        DocGen.HTML.paragraph(sb, validation);
      }

      if (isClassifier() && nclasses() != 2 /*binary classifier has its own conflicting D3 object (AUC)*/) {
        // Plot training error
        float[] err = new float[errors.length];
        float[] samples = new float[errors.length];
        for (int i=0; i<err.length; ++i) {
          err[i] = (float)errors[i].train_err;
          samples[i] = errors[i].training_samples;
        }
        new D3Plot(samples, err, "training samples", "classification error",
                "classification error on training data").generate(sb);

        // Plot validation error
        if (model_info.parameters.validation != null) {
          for (int i=0; i<err.length; ++i) {
            err[i] = (float)errors[i].valid_err;
          }
          new D3Plot(samples, err, "training samples", "classification error",
                  "classification error on validation set").generate(sb);
        }
      }
      // regression
      else if (!isClassifier()) {
        // Plot training MSE
        float[] err = new float[errors.length-1];
        float[] samples = new float[errors.length-1];
        for (int i=0; i<err.length; ++i) {
          err[i] = (float)errors[i+1].train_mse;
          samples[i] = errors[i+1].training_samples;
        }
        new D3Plot(samples, err, "training samples", "MSE",
                "regression error on training data").generate(sb);

        // Plot validation MSE
        if (model_info.parameters.validation != null) {
          for (int i=0; i<err.length; ++i) {
            err[i] = (float)errors[i+1].valid_mse;
          }
          new D3Plot(samples, err, "training samples", "MSE",
                  "regression error on validation data").generate(sb);
        }
      }
    }

//    String training = "Number of training set samples for scoring: " + error.score_training;
    if (error.validation) {
//      String validation = "Number of validation set samples for scoring: " + error.score_validation;
    }
    sb.append("<table class='table table-striped table-bordered table-condensed'>");
    sb.append("<tr>");
    sb.append("<th>Training Time</th>");
    sb.append("<th>Training Epochs</th>");
    sb.append("<th>Training Samples</th>");
    if (isClassifier()) {
//      sb.append("<th>Training MCE</th>");
      sb.append("<th>Training Error</th>");
      if (nclasses()==2) sb.append("<th>Training AUC</th>");
    } else {
      sb.append("<th>Training MSE</th>");
    }
    if (error.validation) {
      if (isClassifier()) {
//      sb.append("<th>Validation MCE</th>");
        sb.append("<th>Validation Error</th>");
        if (nclasses()==2) sb.append("<th>Validation AUC</th>");
      } else {
        sb.append("<th>Validation MSE</th>");
      }
    }
    sb.append("</tr>");
    for( int i = errors.length - 1; i >= 0; i-- ) {
      final Errors e = errors[i];
      sb.append("<tr>");
      sb.append("<td>" + PrettyPrint.msecs(e.training_time_ms, true) + "</td>");
      sb.append("<td>" + String.format("%g", e.epoch_counter) + "</td>");
      sb.append("<td>" + String.format("%,d", e.training_samples) + "</td>");
      if (isClassifier()) {
//        sb.append("<td>" + String.format(cross_entropy_format, e.train_mce) + "</td>");
        sb.append("<td>" + formatPct(e.train_err) + "</td>");
        if (nclasses()==2) {
          if (e.trainAUC != null) sb.append("<td>" + formatPct(e.trainAUC.AUC()) + "</td>");
          else sb.append("<td>" + "N/A" + "</td>");
        }
      } else {
        sb.append("<td>" + String.format(mse_format, e.train_mse) + "</td>");
      }
      if(e.validation) {
        if (isClassifier()) {
//          sb.append("<td>" + String.format(cross_entropy_format, e.valid_mce) + "</td>");
          sb.append("<td>" + formatPct(e.valid_err) + "</td>");
          if (nclasses()==2) {
            if (e.validAUC != null) sb.append("<td>" + formatPct(e.validAUC.AUC()) + "</td>");
            else sb.append("<td>" + "N/A" + "</td>");
          }
        } else {
          sb.append("<td>" + String.format(mse_format, e.valid_mse) + "</td>");
        }
      }
      sb.append("</tr>");
    }
    sb.append("</table>");
    return true;
  }

  private static String formatPct(double pct) {
    String s = "N/A";
    if( !isNaN(pct) )
      s = String.format("%5.2f %%", 100 * pct);
    return s;
  }

  public boolean toJavaHtml(StringBuilder sb) { return false; }
  @Override public String toJava() { return "Not yet implemented."; }
}

