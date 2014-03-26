package hex.glm;

import hex.ConfusionMatrix;
import hex.glm.GLMParams.Family;

import java.text.DecimalFormat;
import java.util.Arrays;

import water.*;
import water.api.AUC;
import water.api.DocGen;
import water.api.Request.API;
import water.api.RequestBuilders;
import water.util.ModelUtils;

/**
 * Class for GLMValidation.
 *
 * @author tomasnykodym
 *
 */
public class GLMValidation extends Iced {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="")
  final double _ymu;
  @API(help="")
  double residual_deviance;
  @API(help="")
  double null_deviance;
  @API(help="")
  double avg_err;
  @API(help="")
  long nobs;

  @API(help="best decision threshold")
  float best_threshold;

  @API(help="")
  double auc = Double.NaN;

  @API(help="AIC")
  double aic;// internal aic used only for poisson family!
  @API(help="internal aic used only for poisson family!")
  private double _aic2;// internal aic used only for poisson family!
  @API(help="")
  final Key dataKey;
  @API(help="Decision thresholds used to generare confuion matrices, AUC and to find the best thresholds based on user criteria")
  public final float [] thresholds;
  @API(help="")
  ConfusionMatrix [] _cms;
  @API(help="")
  final GLMParams _glm;
  @API(help="")
  final private int _rank;

  public static class GLMXValidation extends GLMValidation {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help="n-fold models built for cross-validation")
    Key [] xval_models;
    public GLMXValidation(GLMModel mainModel, GLMModel [] xvalModels, int lambdaIdx, long nobs) {
      super(mainModel._dataKey, mainModel.ymu, mainModel.glm, mainModel.rank(lambdaIdx));
      xval_models = new Key[xvalModels.length];
      for(int i = 0; i < xvalModels.length; ++i){
        add(xvalModels[i].validation());
        xval_models[i] = xvalModels[i]._key;
      }
      this.nobs = nobs;
      finalize_AIC_AUC();
    }
  }
  public GLMValidation(Key dataKey, double ymu, GLMParams glm, int rank){
    this(dataKey, ymu, glm, rank,glm.family == Family.binomial?ModelUtils.DEFAULT_THRESHOLDS:null);
  }
  public GLMValidation(Key dataKey, double ymu, GLMParams glm, int rank, float [] thresholds){
    _rank = rank;
    _ymu = ymu;
    _glm = glm;
    if(_glm.family == Family.binomial){
      _cms = new ConfusionMatrix[thresholds.length];
      for(int i = 0; i < _cms.length; ++i)
        _cms[i] = new ConfusionMatrix(2);
    }
    this.dataKey = dataKey;
    this.thresholds = thresholds;
  }
  protected void regularize(double reg){avg_err = avg_err*reg;}
  public static Key makeKey(){return Key.make("__GLMValidation_" + Key.make());}
  public void add(double yreal, double ymodel){
    null_deviance += _glm.deviance(yreal, _ymu);
    if(_glm.family == Family.binomial) // classification -> update confusion matrix too
      for(int i = 0; i < thresholds.length; ++i)
        _cms[i].add((int)yreal, (ymodel >= thresholds[i])?1:0);
    residual_deviance  += _glm.deviance(yreal, ymodel);
    ++nobs;
    avg_err += (ymodel - yreal) * (ymodel - yreal);
    if( _glm.family == Family.poisson ) { // aic for poisson
      long y = Math.round(yreal);
      double logfactorial = 0;
      for( long i = 2; i <= y; ++i )
        logfactorial += Math.log(i);
      _aic2 += (yreal * Math.log(ymodel) - logfactorial - ymodel);
    }
  }
  public void add(GLMValidation v){
    residual_deviance  += v.residual_deviance;
    null_deviance += v.null_deviance;
    avg_err = (double)nobs/(nobs+v.nobs)*avg_err +  (double)v.nobs/(nobs+v.nobs)*v.avg_err;
    nobs += v.nobs;
    _aic2 += v._aic2;
    if(_cms == null)_cms = v._cms;
    else for(int i = 0; i < _cms.length; ++i)_cms[i].add(v._cms[i]);
  }
  public final double nullDeviance(){return null_deviance;}
  public final double residualDeviance(){return residual_deviance;}
  public final long nullDOF(){return nobs-1;}
  public final long resDOF(){return nobs - _rank -1;}
  public double auc(){return auc;}
  public double aic(){return aic;}
  protected void computeAIC(){
    aic = 0;
    switch( _glm.family ) {
      case gaussian:
        aic =  nobs * (Math.log(residual_deviance / nobs * 2 * Math.PI) + 1) + 2;
        break;
      case binomial:
        aic = residual_deviance;
        break;
      case poisson:
        aic = -2*_aic2;
        break; // aic is set during the validation task
      case gamma:
      case tweedie:
        aic = Double.NaN;
        break;
      default:
        assert false : "missing implementation for family " + _glm.family;
    }
    aic += 2*_rank;
  }
  @Override
  public String toString(){
    return "null_dev = " + null_deviance + ", res_dev = " + residual_deviance + ", auc = " + auc();
  }

  protected void finalize_AIC_AUC(){
    computeAIC();
    if(_glm.family == Family.binomial){
      AUC auc = new AUC(_cms,thresholds);
      this.auc = auc.AUC();
      best_threshold = auc.threshold();
    }
  }
  /**
   * Computes area under the ROC curve. The ROC curve is computed from the confusion matrices
   * (there is one for each computed threshold). Area under this curve is then computed as a sum
   * of areas of trapezoids formed by each neighboring points.
   *
   * @return estimate of the area under ROC curve of this classifier.
   */
  double[] tprs;
  double[] fprs;

  private double trapeziod_area(double x1, double x2, double y1, double y2) {
    double base = Math.abs(x1 - x2);
    double havg = 0.5 * (y1 + y2);
    return base * havg;
  }

}
