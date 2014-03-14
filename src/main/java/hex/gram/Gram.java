package hex.gram;

import Jama.CholeskyDecomposition;
import Jama.Matrix;
import hex.FrameTask;
import hex.glm.LSMSolver.ADMMSolver.NonSPDMatrixException;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.Futures;
import water.Iced;
import water.Job;
import water.MemoryManager;
import water.util.Log;
import water.util.Utils;

import java.util.Arrays;

public final class Gram extends Iced {
  final boolean _hasIntercept;
  public double[][] _xx;
  double[] _diag;
  final int _diagN;
  final int _denseN;
  final int _fullN;
  final static int MIN_TSKSZ=10000;

  public Gram() {_diagN = _denseN = _fullN = 0; _hasIntercept = false; }

  public Gram(int N, int diag, int dense, int sparse, boolean hasIntercept) {
    _hasIntercept = hasIntercept;
    _fullN = N + (_hasIntercept?1:0);
    _xx = new double[_fullN - diag][];
    _diag = MemoryManager.malloc8d(_diagN = diag);
    _denseN = dense;
    for( int i = 0; i < (_fullN - _diagN); ++i )
      _xx[i] = MemoryManager.malloc8d(diag + i + 1);
  }

  public Gram(Gram g){
    _diagN = g._diagN;
    _denseN = g._denseN;
    _fullN = g._fullN;
    _hasIntercept = g._hasIntercept;
    if(g._diag != null)_diag = g._diag.clone();
    if(g._xx != null){
      _xx = g._xx.clone();
      for(int i = 0; i < _xx.length; ++i)
        _xx[i] = _xx[i].clone();
    }
  }

  public final int fullN(){return _fullN;}
  public double _diagAdded;
  public void addDiag(double d) {addDiag(d,false);}
  public void addDiag(double d, boolean add2Intercept) {
    _diagAdded += d;
    for( int i = 0; i < _diag.length; ++i )
      _diag[i] += d;
    int ii = (!_hasIntercept || add2Intercept)?0:1;
    for( int i = 0; i < _xx.length - ii; ++i )
      _xx[i][_xx[i].length - 1] += d;
  }

  public double diagMin(){
    double res = Double.POSITIVE_INFINITY;
    if(_diag != null)
      for(double d:_diag) if(d < res)res = d;
    if(_xx != null)
      for(double [] x:_xx)if(x[x.length-1] < res)res = x[x.length-1];
    return res;
  }
  @Override
  public Gram clone(){return new Gram(this);}
  public String toString(){
    if(_fullN >= 1000){
      if(_denseN >= 1000) return "Gram(" + _fullN + ")";
      else return "diag:\n" + Arrays.toString(_diag) + "\ndense:\n" + Utils.pprint(getDenseXX());
    } else return Utils.pprint(getXX());
  }
  static public class InPlaceCholesky {
    final double _xx[][];             // Lower triangle of the symmetric matrix.
    private boolean _isSPD;
    private InPlaceCholesky(double xx[][], boolean isspd) { _xx = xx; _isSPD = isspd; }
    static private class BlockTask extends RecursiveAction {
      final double[][] _xx;
      final int _i0, _i1, _j0, _j1;
      public BlockTask(double xx[][], int ifr, int ito, int jfr, int jto) {
        _xx = xx;
        _i0 = ifr; _i1 = ito; _j0 = jfr; _j1 = jto;
      }
      @Override public void compute() {
        for (int i=_i0; i < _i1; i++) {
          double rowi[] = _xx[i];
          for (int k=_j0; k < _j1; k++) {
            double rowk[] = _xx[k];
            double s = 0.0;
            for (int jj = 0; jj < k; jj++) s += rowk[jj]*rowi[jj];
            rowi[k] = (rowi[k] - s) / rowk[k];
          }
        }
      }
    }
    public static InPlaceCholesky decompose_2(double xx[][], int STEP, int P) {
      boolean isspd = true;
      final int N = xx.length;
      P = Math.max(1, P);
      for (int j=0; j < N; j+=STEP) {
        // update the upper left triangle.
        final int tjR = Math.min(j+STEP, N);
        for (int i=j; i < tjR; i++) {
          double rowi[] = xx[i];
          double d = 0.0;
          for (int k=j; k < i; k++) {
            double rowk[] = xx[k];
            double s = 0.0;
            for (int jj = 0; jj < k; jj++) s += rowk[jj]*rowi[jj];
            rowi[k] = s = (rowi[k] - s) / rowk[k];
            d += s*s;
          }
          for (int jj = 0; jj < j; jj++) { double s = rowi[jj]; d += s*s; }
          d = rowi[i] - d;
          isspd = isspd && (d > 0.0);
          rowi[i] = Math.sqrt(Math.max(0.0, d));
        }
        if (tjR == N) break;
        // update the lower strip
        int i = tjR;
        Futures fs = new Futures();
        int rpb = 0;                // rows per block
        int p = P;                  // concurrency
        while ( tjR*(rpb=(N - tjR)/p)<Gram.MIN_TSKSZ && p>1) --p;
        while (p-- > 1) {
          fs.add(new BlockTask(xx,i,i+rpb,j,tjR).fork());
          i += rpb;
        }
        new BlockTask(xx,i,N,j,tjR).compute();
        fs.blockForPending();
      }
      return new InPlaceCholesky(xx, isspd);
    }
    public double[][] getL() { return _xx; }
    public boolean isSPD() { return _isSPD; }
  }

  public Cholesky cholesky(Cholesky chol) {
    return cholesky(chol,true,"");
  }
  /**
   * Compute the cholesky decomposition.
   *
   * In case our gram starts with diagonal submatrix of dimension N, we exploit this fact to reduce the complexity of the problem.
   * We use the standard decomposition of the cholesky factorization into submatrices.
   *
   * We split the Gram into 3 regions (4 but we only consider lower diagonal, sparse means diagonal region in this context):
   *     diagonal
   *     diagonal*dense
   *     dense*dense
   * Then we can solve the cholesky in 3 steps:
   *  1. We solve the diagnonal part right away (just do the sqrt of the elements).
   *  2. The diagonal*dense part is simply divided by the sqrt of diagonal.
   *  3. Compute Cholesky of dense*dense - outer product of cholesky of diagonal*dense computed in previous step
   *
   * @param chol
   * @return
   */
  public Cholesky cholesky(Cholesky chol, boolean parallelize,String id) {
    long start = System.currentTimeMillis();
    if( chol == null ) {
      double[][] xx = _xx.clone();
      for( int i = 0; i < xx.length; ++i )
        xx[i] = xx[i].clone();
      chol = new Cholesky(xx, _diag.clone());
    }
    final Cholesky fchol = chol;
    final int sparseN = _diag.length;
    final int denseN = _fullN - sparseN;
    // compute the cholesky of the diagonal and diagonal*dense parts
    if( _diag != null ) for( int i = 0; i < sparseN; ++i ) {
      double d = 1.0 / (chol._diag[i] = Math.sqrt(_diag[i]));
      for( int j = 0; j < denseN; ++j )
        chol._xx[j][i] = d*_xx[j][i];
    }
    ForkJoinTask [] fjts = new ForkJoinTask[denseN];
    // compute the outer product of diagonal*dense
    //Log.info("SPARSEN = " + sparseN + "    DENSEN = " + denseN);
    final int[][] nz = new int[denseN][];
    for( int i = 0; i < denseN; ++i ) {
      final int fi = i;
      fjts[i] = new RecursiveAction() {
        @Override protected void compute() {
          int[] tmp = new int[sparseN];
          double[] rowi = fchol._xx[fi];
          int n = 0;
          for( int k = 0; k < sparseN; ++k )
            if (rowi[k] != .0) tmp[n++] = k;
          nz[fi] = Arrays.copyOf(tmp, n);
        }
      };
    }
    ForkJoinTask.invokeAll(fjts);
    for( int i = 0; i < denseN; ++i ) {
      final int fi = i;
      fjts[i] = new RecursiveAction() {
          @Override protected void compute() {
            double[] rowi = fchol._xx[fi];
            int[]    nzi  = nz[fi];
            for( int j = 0; j <= fi; ++j ) {
              double[] rowj = fchol._xx[j];
              int[]    nzj  = nz[j];
              double s = 0;
              for (int t=0,z=0; t < nzi.length && z < nzj.length; ) {
                int k1 = nzi[t];
                int k2 = nzj[z];
                if (k1 < k2) { t++; continue; }
                else if (k1 > k2) { z++; continue; }
                else {
                  s += rowi[k1] * rowj[k1];
                  t++; z++;
                }
              }
              rowi[j + sparseN] = _xx[fi][j + sparseN] - s;
            }
          }
        };
    }
    ForkJoinTask.invokeAll(fjts);
    // compute the cholesky of dense*dense-outer_product(diagonal*dense)
    // TODO we still use Jama, which requires (among other things) copy and expansion of the matrix. Do it here without copy and faster.
    double[][] arr = new double[denseN][];
    for( int i = 0; i < arr.length; ++i )
      arr[i] = Arrays.copyOfRange(fchol._xx[i], sparseN, sparseN + denseN);

    Log.info(id + ": CHOLESKY PRECOMPUTE TIME " + (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    // parallelize cholesky
    if (parallelize) {
      int p = Runtime.getRuntime().availableProcessors();
      InPlaceCholesky d = InPlaceCholesky.decompose_2(arr, 10, p);
      fchol.setSPD(d.isSPD());
      arr = d.getL();
      Log.info (id + ": H2O CHOLESKY DECOMP TAKES: " + (System.currentTimeMillis()-start));
    } else {
      // make it symmetric
      for( int i = 0; i < arr.length; ++i )
        for( int j = 0; j < i; ++j )
          arr[j][i] = arr[i][j];
      CholeskyDecomposition c = new Matrix(arr).chol();
      fchol.setSPD(c.isSPD());
      arr = c.getL().getArray();
      //Log.info ("JAMA CHOLESKY DECOMPOSE TAKES: " + (System.currentTimeMillis()-start));
    }
    for( int i = 0; i < arr.length; ++i )
      System.arraycopy(arr[i], 0, fchol._xx[i], sparseN, i + 1);
    return chol;
  }

  public double[][] getXX() {
    final int N = _fullN;
    double[][] xx = new double[N][];
    for( int i = 0; i < N; ++i )
      xx[i] = MemoryManager.malloc8d(N);
    for( int i = 0; i < _diag.length; ++i )
      xx[i][i] = _diag[i];
    for( int i = 0; i < _xx.length; ++i ) {
      for( int j = 0; j < _xx[i].length; ++j ) {
        xx[i + _diag.length][j] = _xx[i][j];
        xx[j][i + _diag.length] = _xx[i][j];
      }
    }
    return xx;
  }

  public double[][] getDenseXX() {
    final int N = _denseN;
    double[][] xx = new double[N][];
    for( int i = 0; i < N; ++i )
      xx[i] = MemoryManager.malloc8d(N);
    for( int i = 0; i < _xx.length; ++i ) {
      for( int j = _diagN; j < _xx[i].length; ++j ) {
        xx[i][j-_diagN] = _xx[i][j];
        xx[j-_diagN][i] = _xx[i][j];
      }
    }
    return xx;
  }


  public void add(Gram grm) {
    Utils.add(_xx,grm._xx);
    Utils.add(_diag,grm._diag);
  }

  public final boolean hasNaNsOrInfs() {
    for( int i = 0; i < _xx.length; ++i )
      for( int j = 0; j < _xx[i].length; ++j )
        if( Double.isInfinite(_xx[i][j]) || Double.isNaN(_xx[i][j]) ) return true;
    for( double d : _diag )
      if( Double.isInfinite(d) || Double.isNaN(d) ) return true;
    return false;
  }

  public static final class Cholesky {
    public final double[][] _xx;
    protected final double[] _diag;
    private boolean _isSPD;

    Cholesky(double[][] xx, double[] diag) {
      _xx = xx;
      _diag = diag;
    }

    public Cholesky(Gram gram) {
      _xx = gram._xx.clone();
      for( int i = 0; i < _xx.length; ++i )
        _xx[i] = gram._xx[i].clone();
      _diag = gram._diag.clone();

    }

    @Override
    public String toString() {
      return "";
    }

    /**
     * Find solution to A*x = y.
     *
     * Result is stored in the y input vector. May throw NonSPDMatrix exception in case Gram is not
     * positive definite.
     *
     * @param y
     */
    public final void   solve(double[] y) {
      long t = System.currentTimeMillis();
      if( !isSPD() ) throw new NonSPDMatrixException();
      assert _xx.length + _diag.length == y.length:"" + _xx.length + " + " + _diag.length + " != " + y.length;

      // diagonal
      for( int k = 0; k < _diag.length; ++k )
        y[k] /= _diag[k];
      // rest
      final int n = y.length;
      // Solve L*Y = B;
      for( int k = _diag.length; k < n; ++k ) {
        for( int i = 0; i < k; i++ )
          y[k] -= y[i] * _xx[k - _diag.length][i];
        y[k] /= _xx[k - _diag.length][k];
      }
      // Solve L'*X = Y;
      for( int k = n - 1; k >= _diag.length; --k ) {
        y[k] /= _xx[k - _diag.length][k];
        for( int i = 0; i < k; ++i )
          y[i] -= y[k] * _xx[k - _diag.length][i];
      }
      // diagonal
      for( int k = _diag.length - 1; k >= 0; --k )
        y[k] /= _diag[k];
    }
    public final boolean isSPD() {return _isSPD;}
    public final void setSPD(boolean b) {_isSPD = b;}
  }

  public final void addRow(final double[] x, final int catN, final int [] catIndexes, final double w) {
    final int intercept = _hasIntercept?1:0;
    final int denseRowStart = _fullN - _denseN - _diagN - intercept; // we keep dense numbers at the right bottom of the matrix, -1 is for intercept
    final int denseColStart = _fullN - _denseN - intercept;

    assert _denseN + denseRowStart == _xx.length-intercept;
    final double [] interceptRow = _hasIntercept?_xx[_denseN + denseRowStart]:null;
    // nums
    for(int i = 0; i < _denseN; ++i) if(x[i] != 0) {
      final double [] mrow = _xx[i+denseRowStart];
      final double d = w*x[i];
      for(int j = 0; j <= i; ++j)if(x[j] != 0)
        mrow[j+denseColStart] += d*x[j];
      if(_hasIntercept)
        interceptRow[i+denseColStart] += d; // intercept*x[i]
      // nums * cats
      for(int j = 0; j < catN; ++j)
        mrow[catIndexes[j]] += d;
    }
    if(_hasIntercept){
      // intercept*intercept
      interceptRow[_denseN+denseColStart] += w;
      // intercept X cat
      for(int j = 0; j < catN; ++j)
        interceptRow[catIndexes[j]] += w;
    }
    final boolean hasDiag = (_diagN > 0 && catN > 0 && catIndexes[0] < _diagN);
    // cat X cat
    for(int i = hasDiag?1:0; i < catN; ++i){
      final double [] mrow = _xx[catIndexes[i] - _diagN];
      for(int j = 0; j <= i; ++j)
        mrow[catIndexes[j]] += w;
    }
    // DIAG
    if(hasDiag)_diag[catIndexes[0]] += w;
  }
  public void mul(double x){
    if(_diag != null)for(int i = 0; i < _diag.length; ++i)
      _diag[i] *= x;
    for(int i = 0; i < _xx.length; ++i)
      for(int j = 0; j < _xx[i].length; ++j)
        _xx[i][j] *= x;
  }

  public double [] mul(double [] x){
    double [] res = MemoryManager.malloc8d(x.length);
    mul(x,res);
    return res;
  }

  public void mul(double [] x, double [] res){
    Arrays.fill(res,0);
    for(int i = 0; i < _diagN; ++i)
      res[i] = x[i] * _diag[i];
    for(int ii = 0; ii < _xx.length; ++ii){
      final int n = _xx[ii].length-1;
      final int i = _diagN + ii;
      for(int j = 0; j < n; ++j) {
        double e = _xx[ii][j];  // we store only lower diagonal, so we have two updates:
        res[i] += x[j]*e;       // standard matrix mul, row * vec, except short (only up to diag)
        res[j] += x[i]*e;       // symmetric matrix => each non-diag element adds to 2 places
      }
      res[i] += _xx[ii][n]*x[n]; // diagonal element
    }
  }
  /**
   * Task to compute gram matrix normalized by the number of observations (not counting rows with NAs).
   * in R's notation g = t(X)%*%X/nobs, nobs = number of rows of X with no NA.
   * @author tomasnykodym
   */
  public static class GramTask extends FrameTask<GramTask> {
    public Gram _gram;
    public double [][] _XY;
    public long _nobs;
    public final boolean _hasIntercept;
    public final boolean _isWeighted; // last response is weigth vector?

    public GramTask(Job job, DataInfo dinfo, boolean hasIntercept, boolean isWeighted){
      super(job,dinfo);
      _hasIntercept = hasIntercept;
      _isWeighted = isWeighted;
    }
    @Override protected void chunkInit(){
      _gram = new Gram(_dinfo.fullN(), _dinfo.largestCat(), _dinfo._nums, _dinfo._cats,_hasIntercept);
      final int responses = _dinfo._responses - (_isWeighted?1:0);
      if(responses > 0){
        _XY = new double[responses][];
        for(int i = 0; i < responses; ++i)
          _XY[i] = MemoryManager.malloc8d(_gram._fullN);
      }
    }
    @Override protected void processRow(long gid, double[] nums, int ncats, int[] cats, double [] responses) {
      double w = _isWeighted?responses[responses.length-1]:1;
      _gram.addRow(nums, ncats, cats, w);
      if(_XY != null){
        for(int i = 0 ; i < _XY.length; ++i){
          final double y = responses[i]*w;
          for(int j = 0; j < ncats; ++i)
            _XY[i][cats[j]] += y;
          int numoff = _dinfo.numStart();
          for(int j = 0; j < nums.length; ++j)
            _XY[i][numoff+j] += nums[j]*y;
        }
      }
      ++_nobs;
    }
    @Override protected void chunkDone(){
      double r = 1.0/_nobs;
      _gram.mul(r);
      if(_XY != null)for(int i = 0; i < _XY.length; ++i)
        for(int j = 0; j < _XY[i].length; ++j)
          _XY[i][j] *= r;
    }
    @Override public void reduce(GramTask gt){
      double r1 = (double)_nobs/(_nobs+gt._nobs);
      _gram.mul(r1);
      double r2 = (double)gt._nobs/(_nobs+gt._nobs);
      gt._gram.mul(r2);
      _gram.add(gt._gram);
      if(_XY != null)for(int i = 0; i < _XY.length; ++i)
        for(int j = 0; j < _XY[i].length; ++j)
          _XY[i][j] = _XY[i][j]*r1 + gt._XY[i][j]*r2;
      _nobs += gt._nobs;
    }
  }
}

