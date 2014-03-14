package hex.glm;

import hex.FrameTask;
import hex.glm.GLMParams.Family;
import hex.gram.Gram;
import water.Job;
import water.MemoryManager;
import water.H2O.H2OCountedCompleter;
import water.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Contains all GLM related tasks.
 *
 * @author tomasnykodym
 *
 */

public abstract class GLMTask<T extends GLMTask<T>> extends FrameTask<T> {
  final protected GLMParams _glm;
  public GLMTask(Job job, DataInfo dinfo, GLMParams glm){this(job,dinfo,glm,null);}
  public GLMTask(Job job, DataInfo dinfo, GLMParams glm,H2OCountedCompleter cmp){super(job,dinfo,cmp);_glm = glm;}

  //helper function to compute eta - i.e. beta * row
  protected final double computeEta(final int ncats, final int [] cats, final double [] nums, final double [] beta){
    double res = 0;
    for(int i = 0; i < ncats; ++i)res += beta[cats[i]];
    final int numStart = _dinfo.numStart();
    for(int i = 0; i < nums.length; ++i)res += nums[i]*beta[numStart+i];
    res += beta[beta.length-1]; // intercept
    return res;
  }
  /**
   * Helper task to compute precise mean of response and number of observations.
   * (We skip rows with NAs, so we can't use Vec's mean in general.
   *
   * @author tomasnykodym
   *
   */
  static class YMUTask extends FrameTask<YMUTask>{
    private long   _nobs;
    protected double _ymu;
    public double _ymin = Double.POSITIVE_INFINITY;
    public double _ymax = Double.NEGATIVE_INFINITY;
    public YMUTask(Job job, DataInfo dinfo) {this(job,dinfo,null);}
    public YMUTask(Job job, DataInfo dinfo, H2OCountedCompleter cmp) {
      super(job,dinfo,cmp);
    }
    @Override protected void processRow(long gid, double[] nums, int ncats, int[] cats, double [] responses) {
      double response = responses[0];
      _ymu += response;
      if(response < _ymin)_ymin = response;
      if(response > _ymax)_ymax = response;
      ++_nobs;
    }
    @Override public void reduce(YMUTask t){
      _ymu = _ymu*((double)_nobs/(_nobs+t._nobs)) + t._ymu*t._nobs/(_nobs+t._nobs);
      _nobs += t._nobs;
      _ymax = Math.max(_ymax,t._ymax);
      _ymin = Math.min(_ymin,t._ymin);
    }
    @Override protected void chunkDone(){_ymu /= _nobs;}
    public double ymu(){return _ymu;}
    public long nobs(){return _nobs;}
  }
  /**
   * Task to compute Lambda Max for the given dataset.
   * @author tomasnykodym
   */
  static class LMAXTask extends GLMTask<LMAXTask> {
    private double[] _z;
    private final double _ymu;
    private final double _gPrimeMu;
    private long _nobs;
    private final int _n;
    private final double _alpha;
    GLMValidation _val;


    public LMAXTask(Job job, DataInfo dinfo, GLMParams glm, double ymu, double alpha, H2OCountedCompleter cmp) {
      super(job,dinfo,glm,cmp);
      _ymu = ymu;
      _gPrimeMu = glm.linkDeriv(ymu);
      _n = dinfo.fullN();
      _alpha = alpha;
    }
    @Override public void chunkInit(){
      _z = MemoryManager.malloc8d(_n);
      _val = new GLMValidation(null,_ymu,_glm,0);
    }
    @Override protected void processRow(long gid, double[] nums, int ncats, int[] cats, double [] responses) {
      double w = (responses[0] - _ymu) * _gPrimeMu;
      for( int i = 0; i < ncats; ++i ) _z[cats[i]] += w;
      final int numStart = _dinfo.numStart();
      ++_nobs;
      for(int i = 0; i < nums.length; ++i)
        _z[i+numStart] += w*nums[i];
      _val.add(responses[0],_ymu);
    }
    @Override public void reduce(LMAXTask l){
      Utils.add(_z, l._z); _nobs += l._nobs;
      _val.add(l._val);
    }
    public double lmax(){
      double res = Math.abs(_z[0]);
      for( int i = 1; i < _z.length; ++i )
        res = Math.max(res, Math.abs(_z[i]));
      return _glm.variance(_ymu) * res / (_nobs*Math.max(_alpha,1e-3));
    }
  }

  /**
   * One iteration of glm, computes weighted gram matrix and t(x)*y vector and t(y)*y scalar.
   *
   * @author tomasnykodym
   *
   */
  public static class GLMLineSearchTask extends GLMTask<GLMLineSearchTask> {
    double [][] _betas;
    double []   _objvals;
    double _caseVal = 0;
    public GLMLineSearchTask(Job job, DataInfo dinfo, GLMParams glm, double [] oldBeta, double [] newBeta, double minStep, H2OCountedCompleter cmp){
      super(job,dinfo,glm,cmp);
      ArrayList<double[]> betas = new ArrayList<double[]>();
      double step = 0.5;
      while(step >= minStep){
        double [] b = MemoryManager.malloc8d(oldBeta.length);
        for(int i = 0; i < oldBeta.length; ++i)
          b[i] = 0.5*(oldBeta[i] + newBeta[i]);
        betas.add(b);
        newBeta = b;
        step *= 0.5;
      }
      _betas = new double[betas.size()][];
      betas.toArray(_betas);
    }
    @Override public void chunkInit(){
      _objvals = new double[_betas.length];
    }
    @Override public final void processRow(long gid, final double [] nums, final int ncats, final int [] cats, double [] responses){
      for(int i = 0; i < _objvals.length; ++i){
        final double [] beta = _betas[i];
        double y = responses[0];
        _objvals[i] += _glm.deviance(y,_glm.linkInv(computeEta(ncats, cats,nums,beta)));
      }
    }
    @Override
    public void reduce(GLMLineSearchTask git){Utils.add(_objvals,git._objvals);}
  }
  /**
   * One iteration of glm, computes weighted gram matrix and t(x)*y vector and t(y)*y scalar.
   *
   * @author tomasnykodym
   *
   */
  public static class GLMIterationTask extends GLMTask<GLMIterationTask> {
    final double [] _beta;
    Gram      _gram;
    double [] _xy;
    double [] _grad;
    double    _yy;
    GLMValidation _val; // validation of previous model
    final double _ymu;
    protected final double _reg;
    long _n;

    public GLMIterationTask(Job job, DataInfo dinfo, GLMParams glm, double [] beta, double ymu, double reg, H2OCountedCompleter cmp) {
      super(job, dinfo,glm,cmp);
      _beta = beta;
      _ymu = ymu;
      _reg = reg;
    }


    @Override public final void processRow(long gid, final double [] nums, final int ncats, final int [] cats, double [] responses){
      ++_n;
      double y = responses[0];
      assert ((_glm.family != Family.gamma) || y > 0) : "illegal response column, y must be > 0  for family=Gamma.";
      assert ((_glm.family != Family.binomial) || (0 <= y && y <= 1)) : "illegal response column, y must be <0,1>  for family=Binomial. got " + y;
      final double w, eta, mu, var, z;
      final int numStart = _dinfo.numStart();
      double d = 0;
      if( _glm.family == Family.gaussian) {
        w = 1;
        z = y;
        assert _beta == null; // don't expect beta here, gaussian is non-iterative
        for(int i = 0; i < ncats; ++i)
          _grad[cats[i]] -= y;
        for(int i = 0; i < nums.length; ++i)_grad[numStart+i] -= y*nums[i];
        mu = 0;
      } else {
        if( _beta == null ) {
          mu = _glm.mustart(y, _ymu);
          eta = _glm.link(mu);
        } else {
          eta = computeEta(ncats, cats,nums,_beta);
          mu = _glm.linkInv(eta);
        }
        _val.add(y, mu);
        var = Math.max(1e-5, _glm.variance(mu)); // avoid numerical problems with 0 variance
        d = _glm.linkDeriv(mu);
        z = eta + (y-mu)*d;
        w = 1.0/(var*d*d);
      }
      assert w >= 0 : "invalid weight " + w;
      final double wz = w * z;
      _yy += wz * z;

      final double grad = w*d*(mu-y);
      for(int i = 0; i < ncats; ++i){
        _grad[cats[i]] += grad;
        _xy[cats[i]] += wz;
      }

      for(int i = 0; i < nums.length; ++i){
        _xy[numStart+i] += wz*nums[i];
        _grad[numStart+i] += grad*nums[i];
      }
      _grad[numStart + _dinfo._nums] += grad;
      _xy[numStart + _dinfo._nums] += wz;
      _gram.addRow(nums, ncats, cats, w);
    }
    @Override protected void chunkInit(){
      _gram = new Gram(_dinfo.fullN(), _dinfo.largestCat(), _dinfo._nums, _dinfo._cats,true);
      _xy = MemoryManager.malloc8d(_dinfo.fullN()+1); // + 1 is for intercept
      _grad = MemoryManager.malloc8d(_dinfo.fullN()+1); // + 1 is for intercept
      int rank = 0;
      if(_beta != null)for(double d:_beta)if(d != 0)++rank;
      _val = new GLMValidation(null,_ymu, _glm,rank);
    }
    @Override protected void chunkDone(){
      _gram.mul(_reg);
      _val.regularize(_reg);
      for(int i = 0; i < _xy.length; ++i)
        _xy[i] *= _reg;
      _yy *= _reg;
    }
    @Override
    public void reduce(GLMIterationTask git){
      Utils.add(_xy, git._xy);
      Utils.add(_grad,git._grad);
      _gram.add(git._gram);
      _yy += git._yy;
      _val.add(git._val);
      _n += git._n;
      super.reduce(git);
    }
  }
}
