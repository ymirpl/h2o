package hex.glm;

import hex.gram.Gram;
import hex.gram.Gram.Cholesky;

import java.util.ArrayList;
import java.util.Arrays;

import jsr166y.RecursiveAction;
import water.Iced;
import water.Key;
import water.MemoryManager;
import water.util.Log;
import water.util.Utils;

import com.google.gson.JsonObject;


/**
 * Distributed least squares solvers
 * @author tomasnykodym
 *
 */
public abstract class LSMSolver extends Iced{

  public enum LSMSolverType {
    AUTO, // AUTO: (len(beta) < 1000)?ADMM:GenGradient
    ADMM,
    GenGradient
  }
  double _lambda;
  final double _alpha;
  public Key _jobKey;
  public String _id;

  public LSMSolver(double lambda, double alpha){
    _lambda = lambda;
    _alpha  = alpha;
  }

  public final boolean converged(Gram gram, double [] beta, double [] xy){
    return converged(gram,beta,xy,1e-8);
  }
  public final boolean converged(Gram gram, double [] beta, double [] xy, final double eps){
    double [] grad = gram.mul(beta);
    double l1pen = _alpha*_lambda;
    double l2pen = (1-_alpha)*_lambda;
    boolean converged = true;
    for(int j = 0; j < grad.length-1; ++j){
      grad[j] = grad[j] - xy[j] + (1-_alpha)*_lambda*beta[j];
      double g = grad[j];
      if(beta[j] == 0){
        g = Math.abs(g);
        if(g > l1pen && (l1pen-g) > eps){
//          System.out.println("grad[" + j +"] = " + grad[j] + " > " + l1pen);
          converged = false;
        }
      } else if(beta[j] < 0 && Math.abs(g - l1pen) > eps) {
//        System.out.println("grad[" + j +"] - " + l1pen + " = " + (g - l1pen) + " > 0");
        converged = false;
      } else if(beta[j] > 0 && Math.abs(g + l1pen) > eps){
        converged = false;
//        System.out.println("grad[" + j +"] + " + l1pen + " = " + (l1pen + g) + " > 0");
      }
    }
    _converged = converged&&Math.abs(grad[grad.length-1] -= xy[grad.length-1]) < eps;
    return _converged;
  }
  /**
   *  @param xy - guassian: -X'y binomial: -(1/4)X'(XB + (y-p)/(p*1-p))
   *  @param yy - <y,y>/2
   *  @param newBeta - resulting vector of coefficients
   *  @return true if converged
   *
   */
  public abstract boolean solve(Gram gram, double [] xy, double yy, double [] newBeta);

  protected boolean _converged;

  public final boolean converged(){return _converged;}
  public static class LSMSolverException extends RuntimeException {
    public LSMSolverException(String msg){super(msg);}
  }
  public abstract String name();


  protected static double shrinkage(double x, double kappa) {
    return Math.max(0, x - kappa) - Math.max(0, -x - kappa);
  }

  /**
   * Compute least squares objective function value:
   *    lsm_obj(beta) = 0.5*(y - X*b)'*(y - X*b) + l1 + l2
   *                  = 0.5*y'y - (X'y)'*b + 0.5*b'*X'X*b) + l1 + l2
   *    l1 = alpha*lambda*l1norm(beta)
   *    l2 = (1-alpha)*lambda*l2norm(beta)/2
   * @param xy:   X'y
   * @param yy:   0.5*y'y
   * @param beta: b (vector of coefficients)
   * @param xb: X'X*beta
   * @return 0.5*(y - X*b)'*(y - X*b) + l1 + l2
   */
  protected double objectiveVal(double[] xy, double yy, double[] beta, double [] xb) {
    double res = lsm_objectiveVal(xy,yy,beta, xb);
    double l1 = 0, l2 = 0;
    for(int i = 0; i < beta.length; ++i){
      l1 += Math.abs(beta[i]);
      l2 += beta[i]*beta[i];
    }
    return res + _alpha*_lambda*l1 + 0.5*(1-_alpha)*_lambda*l2;
  }

  /**
   * Compute the LSM objective.
   *
   *   lsm_obj(beta) = 0.5 * (y - X*b)' * (y - X*b)
   *                 = 0.5 * y'y - (X'y)'*b + 0.5*b'*X'X*b)
   *                 = 0.5yy + b*(0.5*X'X*b - X'y)
   * @param xy X'y
   * @param yy y'y
   * @param beta
   * @param xb X'X*beta
   * @return
   */
  protected double lsm_objectiveVal(double[] xy, double yy, double[] beta, double [] xb) {
    double res = 0.5*yy;
    for(int i = 0; i < xb.length; ++i)
      res += beta[i]*(0.5*xb[i] - xy[i]);
    return res;
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
  public long _n;
    //public static final double DEFAULT_LAMBDA = 1e-5;
    public static final double DEFAULT_ALPHA = 0.5;
    public double _orlx = 1;//1.4; // over relaxation param
    public double _rho = Double.NaN;
    public double [] _wgiven;
    public double _proximalPenalty;
    private static final double GLM1_RHO = 1.0e-3;

    public boolean normalize() {return _lambda != 0;}

    public double _addedL2;
    public ADMMSolver (double lambda, double alpha) {
      super(lambda,alpha);
    }
    public ADMMSolver (double lambda, double alpha, double addedL2) {
      super(lambda,alpha);
      _addedL2 = addedL2;
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("lambda",_lambda);
      res.addProperty("alpha",_alpha);
      return res;
    }

    public static class NonSPDMatrixException extends LSMSolverException {
      public NonSPDMatrixException(){super("Matrix is not SPD, can't solve without regularization\n");}
      public NonSPDMatrixException(Gram grm){
        super("Matrix is not SPD, can't solve without regularization\n" + grm);
      }
    }

    @Override
    public boolean solve(Gram gram, double [] xy, double yy, double[] z) {
      return solve(gram, xy, yy, z, Double.POSITIVE_INFINITY);
    }

    private static double l1_norm(double [] v){
      double res = 0;
      for(double d:v)res += Math.abs(d);
      return res;
    }
    private static double l2_norm(double [] v){
      double res = 0;
      for(double d:v)res += d*d;
      return res;
    }

    public boolean solve(Gram gram, double [] xy, double yy, double[] z, double objVal) {
      double d = gram._diagAdded;
      final int N = xy.length;
      Arrays.fill(z, 0);
      if(_lambda>0 || _addedL2 > 0)
        gram.addDiag(_lambda*(1-_alpha)/_n + _addedL2);
      double rho = _rho;
      if(_alpha > 0 && _lambda > 0){
        if(Double.isNaN(_rho)) rho = _lambda*_alpha;//gram.diagMin()+1e-5;// find rho value as min diag element + constant
        gram.addDiag(rho);
      }
      if(_proximalPenalty > 0 && _wgiven != null){
        gram.addDiag(_proximalPenalty, true);
        xy = xy.clone();
        for(int i = 0; i < xy.length; ++i)
          xy[i] += _proximalPenalty*_wgiven[i];
      }
      int attempts = 0;
      long t1 = System.currentTimeMillis();
      Cholesky chol = gram.cholesky(null,true,_id);
      long t2 = System.currentTimeMillis();
      while(!chol.isSPD() && attempts < 10){
        if(_addedL2 == 0) _addedL2 = 1e-5;
        else _addedL2 *= 10;
        Log.info("GLM ADMM: BUMPED UP RHO TO " + rho + _addedL2);
        ++attempts;
        gram.addDiag(_addedL2); // try to add L2 penalty to make the Gram issp
        gram.cholesky(chol);
      }
      Log.info(_id + ": Cholesky decomp done in " + (t2-t1) + "ms");
      if(!chol.isSPD()){
        System.out.println("can not solve, got non-spd matrix and adding regularization did not help, matrix = \n" + gram);
        throw new NonSPDMatrixException(gram);
      }
      _rho = rho;
      if(_alpha == 0 || _lambda == 0){ // no l1 penalty
        System.arraycopy(xy, 0, z, 0, xy.length);
        chol.solve(z);
        gram.addDiag(-gram._diagAdded + d);
        return true;
      }
      long t = System.currentTimeMillis();
      final double ABSTOL = Math.sqrt(N) * 1e-8;
      final double RELTOL = 1e-4;
      double[] u = MemoryManager.malloc8d(N);
      double [] xyPrime = xy.clone();
      double kappa = _lambda*_alpha/rho;
      double [] grad = null;
      int i;
      for(i = 0; i < 2500; ++i ) {
        // first compute the x update
        // add rho*(z-u) to A'*y
        for( int j = 0; j < N-1; ++j )xyPrime[j] = xy[j] + rho*(z[j] - u[j]);
        xyPrime[N-1] = xy[N-1];
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
        s_norm = rho * Math.sqrt(s_norm);
        eps_pri = ABSTOL + RELTOL * Math.sqrt(Math.max(x_norm, z_norm));
        eps_dual = ABSTOL + rho * RELTOL * Math.sqrt(u_norm);
        if( r_norm < eps_pri && s_norm < eps_dual){
          double d2 = -gram._diagAdded + d;
          gram.addDiag(d2);
          if(_converged = converged(gram,z,xy,1e-8)) break;
          else gram.addDiag(-d2);
        }
      }
      if(!_converged)gram.addDiag(-gram._diagAdded + d);
      assert gram._diagAdded == d;
      Log.info("ADMM " + (_converged ? "converged" : "done(NOT CONVERGED)") + " in " + i + " iterations and " + (System.currentTimeMillis() - t) + "ms");
      return _converged;
    }
    @Override
    public String name() {return "ADMM";}
  }

  public static final class ProxSolver extends LSMSolver {
    public ProxSolver(double lambda, double alpha){super(lambda,alpha);}


    /**
     * @param newB
     * @param oldObj
     * @param oldB
     * @param oldGrad
     * @param t
     * @return
     */
    private static final double f_hat(double [] newB,double oldObj, double [] oldB,double [] xb, double [] xy, double t){
      double res = oldObj;
      double l2 = 0;
      for(int i = 0; i < newB.length; ++i){
        double diff = newB[i] - oldB[i];
        res += (xb[i]-xy[i])*diff;
        l2 += diff*diff;
      }
      return res + 0.25*l2/t;
    }
  private double penalty(double [] beta){
    double l1 = 0,l2 = 0;
    for(int i = 0; i < beta.length; ++i){
      l1 += Math.abs(beta[i]);
      l2 += beta[i]*beta[i];
    }
    return _lambda*(_alpha*l1 + (1-_alpha)*l2*0.5);
  }
    private static double betaDiff(double [] b1, double [] b2){
      double res = 0;
      for(int i = 0; i < b1.length; ++i)
        Math.max(res, Math.abs(b1[i] - b2[i]));
      return res;
    }
    @Override
    public boolean solve(Gram gram, double [] xy, double yy, double[] beta) {
      ADMMSolver admm = new ADMMSolver(_lambda,_alpha);
      if(gram != null)return admm.solve(gram,xy,yy,beta);
      Arrays.fill(beta,0);
      long t1 = System.currentTimeMillis();
      final double [] xb = gram.mul(beta);
      double objval = objectiveVal(xy,yy,beta,xb);
      final double [] newB = MemoryManager.malloc8d(beta.length);
      final double [] newG = MemoryManager.malloc8d(beta.length);
      double step = 1;
      final double l1pen = _lambda*_alpha;
      final double l2pen = _lambda*(1-_alpha);
      double lsmobjval = lsm_objectiveVal(xy,yy,beta,xb);
      boolean converged = false;
      final int intercept = beta.length-1;
      int iter = 0;
      MAIN:
      while(!converged && iter < 1000) {
        ++iter;
        step = 1;
        while(step > 1e-12){ // line search
          double l2shrink = 1/(1+step*l2pen);
          double l1shrink = l1pen*step;
          for(int i = 0; i < beta.length-1; ++i)
            newB[i] = l2shrink*shrinkage((beta[i]-step*(xb[i]-xy[i])),l1shrink);
          newB[intercept] = beta[intercept] - step*(xb[intercept]-xy[intercept]);
          gram.mul(newB, newG);
          double newlsmobj = lsm_objectiveVal(xy, yy, newB,newG);
          double fhat = f_hat(newB,lsmobjval,beta,xb,xy,step);
          if(newlsmobj <= fhat){
            lsmobjval = newlsmobj;
            converged = betaDiff(beta,newB) < 1e-6;
            System.arraycopy(newB,0,beta,0,newB.length);
            System.arraycopy(newG,0,xb,0,newG.length);
            continue MAIN;
          } else step *= 0.8;
        }
        converged = true;
      }
      System.out.println("Proximal solver done" + " in " + iter + " iterations and " + (System.currentTimeMillis()-t1) + "ms" + ", objval reduced from " + objval + " to " + lsm_objectiveVal(xy,yy,beta,xb));
      converged = converged(gram,beta,xy);
      return converged;
    }
    public String name(){return "ProximalGradientSolver";}
  }
}
