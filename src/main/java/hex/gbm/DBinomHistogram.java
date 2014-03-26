package hex.gbm;

import water.MemoryManager;
import water.util.Utils;

/**
   A Histogram, computed in parallel over a Vec.
   <p>
   Sums (& sums-of-squares) of binomials - 0 or 1.  Sums-of-squares==sums in this case.
   <p>
   @author Cliff Click
*/
public class DBinomHistogram extends DHistogram<DBinomHistogram> {
  private long _sums[]; // Sums (& square-sums since only 0 & 1 allowed), shared, atomically incremented

  public DBinomHistogram( String name, final int nbins, byte isInt, float min, float maxEx, long nelems ) {
    super(name,nbins,isInt,min,maxEx,nelems);
  }
  @Override boolean isBinom() { return true; }

  @Override public double mean(int b) {
    long n = _bins[b];
    return n>0 ? (double)_sums[b]/n : 0;
  }
  @Override public double var (int b) {
    long n = _bins[b];
    if( n<=1 ) return 0;
    return (_sums[b] - (double)_sums[b]*_sums[b]/n)/(n-1);
  }

  // Big allocation of arrays
  @Override void init0() {
    _sums = MemoryManager.malloc8(_nbin);
  }

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response mean & variance.
  @Override void incr0( int b, double y ) {
    Utils.AtomicLongArray.incr(_sums,b);
  }

  // Merge two equal histograms together.  Done in a F/J reduce, so no
  // synchronization needed.
  @Override void add0( DBinomHistogram dsh ) {
    Utils.add(_sums,dsh._sums);
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of the MSEs when the data is split at a single point.
  // mses[1] == MSE for splitting between bins  0  and 1.
  // mses[n] == MSE for splitting between bins n-1 and n.
  @Override public DTree.Split scoreMSE( int col ) {
    final int nbins = nbins();
    assert nbins > 1;

    // Compute mean/var for cumulative bins from 0 to nbins inclusive.
    long sums0[] = MemoryManager.malloc8(nbins+1);
    long   ns0[] = MemoryManager.malloc8(nbins+1);
    for( int b=1; b<=nbins; b++ ) {
      long m0 = sums0[b-1],  m1 = _sums[b-1];
      long k0 = ns0  [b-1],  k1 = _bins[b-1];
      if( k0==0 && k1==0 ) continue;
      sums0[b] = m0+m1;
      ns0  [b] = k0+k1;
    }
    long tot = ns0[nbins];
    // If we see zero variance, we must have a constant response in this
    // column.  Normally this situation is cut out before we even try to split, but we might
    // have NA's in THIS column...
    if( sums0[nbins] == 0 || sums0[nbins] == tot ) { assert isConstantResponse(); return null; }

    // Compute mean/var for cumulative bins from nbins to 0 inclusive.
    long sums1[] = MemoryManager.malloc8(nbins+1);
    long   ns1[] = MemoryManager.malloc8(nbins+1);
    for( int b=nbins-1; b>=0; b-- ) {
      long m0 = sums1[b+1],  m1 = _sums[b];
      long k0 = ns1  [b+1],  k1 = _bins[b];
      if( k0==0 && k1==0 ) continue;
      sums1[b] = m0+m1;
      ns1  [b] = k0+k1;
      assert ns0[b]+ns1[b]==tot;
    }

    // Now roll the split-point across the bins.  There are 2 ways to do this:
    // split left/right based on being less than some value, or being equal/
    // not-equal to some value.  Equal/not-equal makes sense for catagoricals
    // but both splits could work for any integral datatype.  Do the less-than
    // splits first.
    int best=0;                         // The no-split
    double best_se0=Double.MAX_VALUE;   // Best squared error
    double best_se1=Double.MAX_VALUE;   // Best squared error
    boolean equal=false;                // Ranged check
    for( int b=1; b<=nbins-1; b++ ) {
      if( _bins[b] == 0 ) continue; // Ignore empty splits
      // We're making an unbiased estimator, so that MSE==Var.
      // Then Squared Error = MSE*N = Var*N
      //                    = (ssqs/N - mean^2)*N
      //                    = ssqs - N*mean^2
      //                    = ssqs - N*(sum/N)(sum/N)
      //                    = ssqs - sum^2/N
      // For binomial, ssqs == sum, so further reduces:
      //                    = sum  - sum^2/N
      double se0 = sums0[b];  se0 -= se0*se0/ns0[b];
      double se1 = sums1[b];  se1 -= se1*se1/ns1[b];
      if( (se0+se1 < best_se0+best_se1) || // Strictly less error?
          // Or tied MSE, then pick split towards middle bins
          (se0+se1 == best_se0+best_se1 &&
           Math.abs(b -(nbins>>1)) < Math.abs(best-(nbins>>1))) ) {
        best_se0 = se0;   best_se1 = se1;
        best = b;
      }
    }

    // If the min==max, we can also try an equality-based split
    if( _isInt > 0 && _step == 1.0f &&    // For any integral (not float) column
        _maxEx-_min > 2 ) { // Also need more than 2 (boolean) choices to actually try a new split pattern
      for( int b=1; b<=nbins-1; b++ ) {
        if( _bins[b] == 0 ) continue; // Ignore empty splits
        long N =        ns0[b+0] + ns1[b+1];
        if( N == 0 ) continue;
        double sums = sums0[b+0]+sums1[b+1];
        double sumb = _sums[b+0];
        double si = sums - sums*sums/   N    ; // Left+right, excluding 'b'
        double sx = sumb - sumb*sumb/_bins[b]; // Just 'b'
        if( si+sx < best_se0+best_se1 ) { // Strictly less error?
          best_se0 = si;   best_se1 = sx;
          best = b;        equal = true; // Equality check
        }
      }
    }

    if( best==0 ) return null;  // No place to split
    assert best > 0 : "Must actually pick a split "+best;
    long   n0 = !equal ?   ns0[best] :   ns0[best]+  ns1[best+1];
    long   n1 = !equal ?   ns1[best] : _bins[best]              ;
    double p0 = !equal ? sums0[best] : sums0[best]+sums1[best+1];
    double p1 = !equal ? sums1[best] : _sums[best]              ;
    return new DTree.Split(col,best,equal,best_se0,best_se1,n0,n1,p0/n0,p1/n1);
  }

  @Override public long byteSize0() {
    return 8*1 +                // 1 more internal arrays
      24+_sums.length<<3;
  }
}
