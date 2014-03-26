package hex;



import hex.DGLM.Cholesky;
import hex.DGLM.Gram;

import java.util.Arrays;

import water.Iced;
import water.Key;
import water.MemoryManager;

import com.google.gson.JsonObject;
import water.util.Log;

/**
 * Distributed least squares solvers
 * @author tomasnykodym
 *
 */
public class DLSM {

  public enum LSMSolverType {
    AUTO, // AUTO: (len(beta) < 1000)?ADMM:GenGradient
    ADMM,
    GenGradient
  }

  public static abstract class LSMSolver extends Iced {
    public double _lambda;
    public double _alpha;
    public Key _jobKey;

    public LSMSolver(double lambda, double alpha){
      _lambda = lambda;
      _alpha  = alpha;
    }
    /**
     *  @param gram Matrix - weighted gram matrix (X'*X) computed over the data
     *  @param newBeta - resulting vector of coefficients
     *  @return true if converged
     *
     */
    public abstract boolean solve(Gram gram, double [] newBeta);
    public abstract JsonObject toJson();

    protected boolean _converged;
    public final boolean converged(){return _converged;}
    public static class LSMSolverException extends RuntimeException {
      public LSMSolverException(String msg){super(msg);}
    }
    public abstract String name();
  }


  private static double shrinkage(double x, double kappa) {
    return Math.max(0, x - kappa) - Math.max(0, -x - kappa);
  }

  static final double[] mul(double[][] X, double[] y, double[] z) {
    final int M = X.length;
    final int N = y.length;
    for( int i = 0; i < M; ++i ) {
      z[i] = X[i][0] * y[0];
      for( int j = 1; j < N; ++j )
        z[i] += X[i][j] * y[j];
    }
    return z;
  }

  static final double[] mul(double[] x, double a, double[] z) {
    for( int i = 0; i < x.length; ++i )
      z[i] = a * x[i];
    return z;
  }

  static final double[] plus(double[] x, double[] y, double[] z) {
    for( int i = 0; i < x.length; ++i )
      z[i] = x[i] + y[i];
    return z;
  }

  static final double[] minus(double[] x, double[] y, double[] z) {
    for( int i = 0; i < x.length; ++i )
      z[i] = x[i] - y[i];
    return z;
  }

  static final double[] shrink(double[] x, double[] z, double kappa) {
    for( int i = 0; i < x.length - 1; ++i )
      z[i] = shrinkage(x[i], kappa);
    z[x.length - 1] = x[x.length - 1]; // do not penalize intercept!
    return z;
  }



  public static final class ADMMSolver extends LSMSolver {

    public static final double DEFAULT_LAMBDA = 1e-5;
    public static final double DEFAULT_ALPHA = 0.5;
    public double _orlx = 1;//1.4; // over relaxation param
    public double _rho = 1e-3;
    public boolean _autoHandleNonSPDMatrix = false;

    public boolean normalize() {
      return _lambda != 0;
    }

    public ADMMSolver (double lambda, double alpha) {
      super(lambda,alpha);
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("lambda",_lambda);
      res.addProperty("alpha",_alpha);
      return res;
    }

    public static class NonSPDMatrixException extends LSMSolverException {
      public NonSPDMatrixException(){
        super("Matrix is not SPD, can't solve without regularization");
      }
    }

    @Override
    public boolean solve(Gram gram, double[] z) {
      final int N = gram._xy.length;
      Arrays.fill(z, 0);
      if(_lambda>0)gram.addDiag(_lambda*(1-_alpha)*0.5 + _rho);
      int attempts = 0;
      long t = System.currentTimeMillis();
      Cholesky chol = gram.cholesky(null,_jobKey);
      Log.info("GLM(" + _jobKey + ")" + " cholesky decomp took " + (System.currentTimeMillis() - t) + "ms");
      double rhoAdd = 0;
      if(!chol._isSPD && _autoHandleNonSPDMatrix){
        Log.info("GLM(" + _jobKey + ")"  + " got non-spd matrix");
        while(!chol._isSPD && attempts < 10){
          double rhoIncrement = _rho*(1<< ++attempts);
          gram.addDiag(rhoIncrement); // try to add L2 penalty to make the Gram issp
          rhoAdd += rhoIncrement;
          gram.cholesky(chol,_jobKey);
        }
        Log.info("GLM(" + _jobKey + ")" + " cholesky decomp after nonSPD took " + (System.currentTimeMillis() - t) + "ms");
      }
      if(!chol._isSPD) throw new NonSPDMatrixException();
      if(_lambda == 0){
        _alpha = 0;
        _lambda = rhoAdd;
      } else
        _rho += rhoAdd;
      if(_alpha == 0 || _lambda == 0){ // no l1 penalty
        System.arraycopy(gram._xy, 0, z, 0, gram._xy.length);
        chol.solve(z);
        return _converged = true;
      }
      final double ABSTOL = Math.sqrt(N) * 1e-4;
      final double RELTOL = 1e-2;
      double[] u = MemoryManager.malloc8d(N);
      double [] xyPrime = gram._xy.clone();
      double kappa = _lambda*_alpha / _rho;

      t = System.currentTimeMillis();
      for(int i = 0; i < 1000; ++i ) {
        // first compute the x update
        // add rho*(z-u) to A'*y
        for( int j = 0; j < N-1; ++j )xyPrime[j] = gram._xy[j] + _rho * (z[j] - u[j]);
        xyPrime[N-1] = gram._xy[N-1];
        // updated x
        chol.solve(xyPrime);
        // vars to be used for stopping criteria
        double x_norm = 0;
        double z_norm = 0;
        double u_norm = 0;
        double r_norm = 0;
        double s_norm = 0;
        double eps_pri = 0; // epsilon primal
        double eps_dual = 0;
        // compute u and z update
        for( int j = 0; j < N-1; ++j ) {
          double x_hat = xyPrime[j];
          x_norm += x_hat * x_hat;
          x_hat = x_hat * _orlx + (1 - _orlx) * z[j];
          double zold = z[j];
          z[j] = shrinkage(x_hat + u[j], kappa);
          z_norm += z[j] * z[j];
          s_norm += (z[j] - zold) * (z[j] - zold);
          r_norm += (xyPrime[j] - z[j]) * (xyPrime[j] - z[j]);
          u[j] += x_hat - z[j];
          u_norm += u[j] * u[j];
        }
        z[N-1] = xyPrime[N-1];
        // compute variables used for stopping criterium
        r_norm = Math.sqrt(r_norm);
        s_norm = _rho * Math.sqrt(s_norm);
        eps_pri = ABSTOL + RELTOL * Math.sqrt(Math.max(x_norm, z_norm));
        eps_dual = ABSTOL + _rho * RELTOL * Math.sqrt(u_norm);
        if( r_norm < eps_pri && s_norm < eps_dual ){
          Log.info("GLM(" + _jobKey + ")" + " ADMM solve took " + i + " iterations and " + (System.currentTimeMillis() - t) + "ms");
          return _converged = true;
        }
      }
      Log.info("GLM(" + _jobKey + ")" + " ADMM solve DID NOT CONVERGE after 1000 iterations and " + (System.currentTimeMillis() - t) + "ms");
      return false;
    }



    @Override
    public String name() {
      return "ADMM";
    }
  }




  /**
   * Generalized gradient solver for solving LSM problem with combination of L1 and L2 penalty.
   *
   * @author tomasnykodym
   *
   */
  public static final class GeneralizedGradientSolver extends LSMSolver {
    public final double        _kappa;             // _lambda*_alpha
    public final double        _betaEps;
    private double[]           _beta;
    private double[]           _betaGradient;
    double                     _objVal;
    double                     _t          = 1.0;
    int                        _iterations = 0;
    public static final int    MAX_ITER    = 1000;
    public static final double EPS         = 1e-5;

    public GeneralizedGradientSolver(double lambda, double alpha) {
      this(lambda,alpha,1e-5);
    }
    public GeneralizedGradientSolver(double lambda, double alpha, double betaEps) {
      super(lambda,alpha);
      _kappa = _lambda * _alpha;
      _betaEps = betaEps;
    }

    /**
     * Compute least squares objective function value: g(beta) = 0.5*(y - X*b)'*(y
     * - X*b) = 0.5*y'y - (X'y)'*b + 0.5*b'*X'X*b)
     * @param xx: X'X
     * @param xy: -X'y
     * @param yy: 0.5*y'y
     * @param beta: b (vector of coefficients)
     * @return 0.5*(y - X*b)'*(y - X*b)
     */
    private double g_beta(double[][] xx, double[] xy, double yy, double[] beta) {
      final int n = xy.length;
      double res = yy;
      for( int i = 0; i < n; ++i ) {
        double x = 0;
        for( int j = 0; j < n; ++j ){
          x += xx[i][j] * beta[j];
        }
        res += (0.5*x + xy[i]) * beta[i];
      }
      if(!(res >= 0))
        throw new LSMSolverException("Generalized Gradient: Can not solved this problem.");
      return res;
    }

    /*
     * Compute beta gradient.
     */
    private void g_beta_gradient(double[][] xx, double[] xy, double t) {
      mul(xx, _beta, _betaGradient);
      plus(xy, _betaGradient, _betaGradient);
      mul(_betaGradient, -t, _betaGradient);
    }

    /**
     * Compute new beta according to:
     *    B = B -t*G_t(B), where G(t) = (B - S(B-t*gradient(B),lambda,t)
     * @param newBeta: vector to be filled with the new value of beta
     * @param t: step size
     * @return newBeta
     */
    private double[] beta_update(double[] newBeta, double t) {
      // S(b - t*g_beta_gradient(b),_kappa*t)/(1+ (1 - alpha)*0.5*t)
      double div = 1 / (1 + ((1 - _alpha) * 0.5 * _lambda * t));
      shrink(plus(_beta, _betaGradient, newBeta), newBeta, _kappa*t);
      for(int i = 0; i < newBeta.length-1; ++i)
        newBeta[i] *= div;
      return newBeta;
    }

    /**
     * Compute right hand side of Armijo rule updated for generalized gradient.
     * Used as a threshold for backtracking (finding optimal step t).
     *
     * @param gbeta
     * @param newBeta
     * @param t
     * @return
     */
    private double backtrack_cond_rs(double gbeta, double[] newBeta, double t) {
      double norm = 0;
      double zg = 0;
      double t_inv = 1.0 / t;
      for( int i = 0; i < _beta.length; ++i ) {
        double diff = _beta[i] - newBeta[i];
        norm += diff * diff;
        zg += _betaGradient[i] * diff;
      }
      return gbeta + (norm * 0.5 + zg) * t_inv;
    }

    double l1norm(double[] v) {
      double res = Math.abs(v[0]);
      for( int i = 1; i < v.length - 1; ++i )
        res += Math.abs(v[i]);
      return res;
    }


    /**
     * @param gram: Matrix - weighted gram matrix (X'*X) computed over the data
     * @param newBeta: resulting vector of coefficients
     * @return true if converged
     */
    @Override
    public boolean solve(Gram gram, double[] newBeta) {
      double [][] xx = gram.getXX();
      double []   xy = gram._xy;
      double      yy = gram._yy;
      if(_beta == null || _beta.length != newBeta.length){
        _beta = MemoryManager.malloc8d(newBeta.length);
        _betaGradient = MemoryManager.malloc8d(newBeta.length);
      }
      int i = 0;
      _converged = false;
      _iterations = 0;
      _objVal = Double.MAX_VALUE;
      mul(xy, -1, xy);
      while( !_converged && _iterations != MAX_ITER ) {
        double gbeta = g_beta(xx, xy, yy, _beta);
        // use backtracking to find proper step size t
        double t = _t;
        for( int k = 0; k < 100; ++k ) {
          g_beta_gradient(xx, xy, t);
          newBeta = beta_update(newBeta, t);
          if( g_beta(xx, xy, yy, newBeta) <= backtrack_cond_rs(gbeta, newBeta, t) ) {
            if( _t > t ) {
              _t = t;
              break;
            }
            t = 1.25 * t;
          } else {
            _t = t;
            t = 0.8 * t;
          }
        }
        // compare objective function values between the runs
        double newObjVal = g_beta(xx, xy, yy, newBeta) + _kappa * l1norm(newBeta);
        System.arraycopy(newBeta, 0, _beta, 0, _beta.length);
        _converged = (1 - newObjVal / _objVal) <= EPS;
        _objVal = newObjVal;
        _iterations = ++i;
      }
      // return xy back to its original state
      mul(xy, -1, xy);
      return _converged;
    }
    @Override
    public JsonObject toJson() {
      JsonObject json = new JsonObject();
      return json;
    }
    @Override
    public String name() {
      return "GeneralizedGradient";
    }
  }
}
