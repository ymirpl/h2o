package hex;

import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.*;
import water.exec.Flow;
import water.parser.*;
import water.util.Utils;
import water.util.Log;

import java.util.Arrays;
import java.util.Random;

/**
 * Summary of a column.
 */
public class Summary2 extends Iced {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a summary of a fluid-vec frame";

  public static final int    MAX_HIST_SZ = water.parser.Enum.MAX_ENUM_SIZE;
  public static final int    NMAX = 5;
  public static final int    RESAMPLE_SZ = 1000;
  // updated boundaries to be 0.1% 1%...99%, 99.9% so R code didn't have to change
  // ideally we extend the array here, and just update the R extraction of 25/50/75 percentiles
  // note python tests (junit?) may look at result
  public static final double DEFAULT_PERCENTILES[] = {0.001,0.01,0.10,0.25,0.33,0.50,0.66,0.75,0.90,0.99,0.999};
  private static final int   T_REAL = 0;
  private static final int   T_INT  = 1;
  private static final int   T_ENUM = 2;
  public BasicStat           _stat0;     /* Basic Vec stats collected by PrePass. */
  public final int           _type;      // 0 - real; 1 - int; 2 - enum
  public double[]            _mins;
  public double[]            _maxs;
  public double[]            _samples;   // currently, sampling is disabled. see below.
  long                       _gprows;    // non-empty rows per group

  final transient String[]   _domain;
  final transient double     _start;
  final transient double     _start2;
  final transient double     _binsz;
  final transient double     _binsz2;    // 2nd finer grained histogram used for quantile estimates for numerics
  transient int              _len1;      /* Size of filled elements in a chunk. */
  transient double[]         _pctile;


  static abstract class Stats extends Iced {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields

    @API(help="stats type"   ) public String type;
    Stats(String type) { this.type = type; }
  }
  // An internal JSON-output-only class
  @SuppressWarnings("unused")
  static class EnumStats extends Stats {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    public EnumStats( int card ) {
      super("Enum");
      this.cardinality = card;
    }
    @API(help="cardinality"  ) public final int     cardinality;
  }

  static class NumStats extends Stats {
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    public NumStats( double mean, double sigma, long zeros, double[] mins, double[] maxs, double[] pctile) {
      super("Numeric");
      this.mean  = mean;
      this.sd    = sigma;
      this.zeros = zeros;
      this.mins  = mins;
      this.maxs  = maxs;
      this.pctile = pctile;
      this.pct   = DEFAULT_PERCENTILES;
    }
    @API(help="mean"        ) public final double   mean;
    @API(help="sd"          ) public final double   sd;
    @API(help="#zeros"      ) public final long     zeros;
    @API(help="min elements") public final double[] mins; // min N elements
    @API(help="max elements") public final double[] maxs; // max N elements
    @API(help="percentile thresholds" ) public final double[] pct;
    @API(help="percentiles" ) public final double[] pctile;
  }
  // OUTPUTS
  // Basic info
  @API(help="name"        ) public String    colname;
  @API(help="type"        ) public String    type;
  // Basic stats
  @API(help="NAs"         ) public long      nacnt;
  @API(help="Base Stats"  ) public Stats     stats;

  @API(help="histogram start")    public double    hstart;
  @API(help="histogram bin step") public double    hstep;
  @API(help="histogram headers" ) public String[]  hbrk;
  @API(help="histogram bin values") public long[]  hcnt;
  public long[]  hcnt2; // finer histogram. not visible
  public double[]  hcnt2_min; // min actual for each bin
  public double[]  hcnt2_max; // max actual for each bin

  public static class BasicStat extends Iced {
    public long _len;   /* length of vec */
    public long _nas;   /* number of NA's */
    public long _nans;   /* number of NaN's */
    public long _pinfs;   /* number of positive infinity's */
    public long _ninfs;   /* number of positive infinity's */
    public long _zeros;   /* number of zeros */
    public double _min1;   /* if there's -Inf, then -Inf, o/w min2 */
    public double _max1;   /* if there's Inf, then Inf, o/w max2 */
    public double _min2;   /* min of the finite numbers. NaN if there's none. */
    public double _max2;   /* max of the finite numbers. NaN if there's none. */
    public BasicStat( ) {
      _len = 0;
      _nas = 0;
      _nans = 0;
      _pinfs = 0;
      _ninfs = 0;
      _zeros = 0;
      _min1 = Double.NaN;
      _max1 = Double.NaN;
      _min2 = Double.NaN;
      _max2 = Double.NaN;
    }
    public BasicStat add(Chunk chk) {
      _len = chk._len;
      for(int i = 0; i < chk._len; i++) {
        double val;
        if (chk.isNA0(i)) { _nas++; continue; }
        if (Double.isNaN(val = chk.at0(i))) { _nans++; continue; }
        if      (val == Double.POSITIVE_INFINITY) _pinfs++;
        else if (val == Double.NEGATIVE_INFINITY) _ninfs++;
        else {
          _min2 = Double.isNaN(_min2)? val : Math.min(_min2,val);
          _max2 = Double.isNaN(_max2)? val : Math.max(_max2,val);
          if (val == .0) _zeros++;
        }
      }
      return this;
    }
    public BasicStat add(BasicStat other) {
      _len += other._len;
      _nas += other._nas;
      _nans += other._nans;
      _pinfs += other._pinfs;
      _ninfs += other._ninfs;
      _zeros += other._zeros;
      if (Double.isNaN(_min2)) _min2 = other._min2;
      else if (!Double.isNaN(other._min2)) _min2 = Math.min(_min2,other._min2);
      if (Double.isNaN(_max2)) _max2 = other._max2;
      else if (!Double.isNaN(other._max2)) _max2 = Math.max(_max2, other._max2);
      return this;
    }
    public BasicStat finishUp() {
      _min1 = _ninfs>0?               Double.NEGATIVE_INFINITY   /* there's -Inf */
              : !Double.isNaN(_min2)? _min2                      /* min is finite */
              : _pinfs>0?             Double.POSITIVE_INFINITY   /* Only Infs exist */
              :                       Double.NaN;                /* All NaN's or NAs */
      _max1 = _pinfs>0?               Double.POSITIVE_INFINITY   /* there's Inf */
              : !Double.isNaN(_max2)? _max2                      /* max is finite */
              : _ninfs>0?             Double.NEGATIVE_INFINITY   /* Only -Infs exist */
              :                       Double.NaN;                /* All NaN's or NAs */
      return this;
    }

    /**
     * @return number of filled elements, excluding NaN's as well.
     */
    public long len1() {
      return _len - _nas - _nans;
    }
    /**
     * Returns whether the fill density is less than the given percent.
     * @param pct target percent.
     * @param nan if true then NaN is counted as missing.
     * @return true if less than {@code pct} of rows are filled. */
    public boolean isSparse(double pct, boolean nan) {
      assert 0 < pct && pct <= 1;
      return (double)(_len - _nas - (nan?_nans:0)) / _len < pct;
    }
  }

  public static class PrePass extends MRTask2<PrePass> {
    public BasicStat _basicStats[];
    @Override public void map(Chunk[] cs) {
      _basicStats = new BasicStat[cs.length];
      for (int c=0; c < cs.length; c++)
        _basicStats[c] = new BasicStat().add(cs[c]);
    }
    @Override public void reduce(PrePass other){
      for (int c = 0; c < _basicStats.length; c++)
        _basicStats[c].add(other._basicStats[c]);
    }
    public PrePass finishUp() {
      for (BasicStat stat : _basicStats) stat.finishUp();
      return this;
    }
  }
  public static class SummaryTask2 extends MRTask2<SummaryTask2> {
    private BasicStat[] _basics;
    private int _max_qbins;
    public Summary2 _summaries[];
    public SummaryTask2 (BasicStat[] basicStats, int max_qbins) { _basics = basicStats; _max_qbins = max_qbins; }
    @Override public void map(Chunk[] cs) {
      _summaries = new Summary2[cs.length];
      for (int i = 0; i < cs.length; i++)
        _summaries[i] = new Summary2(_fr.vecs()[i], _fr.names()[i], _basics[i], _max_qbins).add(cs[i]);
    }
    @Override public void reduce(SummaryTask2 other) {
      for (int i = 0; i < _summaries.length; i++)
        _summaries[i].add(other._summaries[i]);
    }
  }

  // Entry point for the Flow passes, to allow easy percentiles on filtered GroupBy
  public static class SummaryPerRow extends Flow.PerRow<SummaryPerRow> {
    public final Frame _fr;
    public final Summary2 _summaries[];
    public SummaryPerRow( Frame fr ) { this(fr,null); }
    private SummaryPerRow( Frame fr, Summary2[] sums ) { _fr = fr; _summaries = sums; }
    @Override public void mapreduce( double ds[] ) { 
      for( int i=0; i<ds.length; i++ )
        _summaries[i].add(ds[i]);
    }
    @Override public void reduce( SummaryPerRow that ) { 
      for (int i = 0; i < _summaries.length; i++)
        _summaries[i].add(that._summaries[i]);
    }
    @Override public SummaryPerRow make() {
      Vec[] vecs = _fr.vecs();
      Summary2 sums[] = new Summary2[vecs.length];
      BasicStat basics[] = new PrePass().doAll(_fr).finishUp()._basicStats;
      for( int i=0; i<vecs.length; i++ )
        sums[i] = new Summary2(vecs[i], _fr._names[i], basics[i]);
      return new SummaryPerRow(_fr,sums);
    }
    @Override public String toString() {
      String s = "";
      for( int i=0; i<_summaries.length; i++ )
        s += _fr._names[i]+" "+_summaries[i]+"\n";
      return s;
    }
    public void finishUp() {
      Vec[] vecs = _fr.vecs();
      for (int i = 0; i < vecs.length; i++) 
        _summaries[i].finishUp(vecs[i]);
    }
  }

  @Override public String toString() {
    String s = "";
    if( stats instanceof NumStats ) {
      double pct   [] = ((NumStats)stats).pct   ;
      double pctile[] = ((NumStats)stats).pctile;
      for( int i=0; i<pct.length; i++ )
        s += ""+(pct[i]*100)+"%="+pctile[i]+", ";
    } else {
      s += "cardinality="+((EnumStats)stats).cardinality;
    }
    return s;
  }

  public void finishUp(Vec vec) {
    nacnt = _stat0._nas;
    if (_type == T_ENUM) {
      // Compute majority items for enum data
      computeMajorities();
    } else {
      _pctile = new double[DEFAULT_PERCENTILES.length];
      // never take this choice (summary1 didn't?
      // means (like R) we can have quantiles with values not in the dataset..ok?
      // ok since approximation? not okay if we did exact. Sampled sort is not good enough?
      // was:
      // if (_samples != null) {
      if (false) { // don't use sampling
        // FIX! should eventually get rid of this since unused
        Arrays.sort(_samples);
        // Compute percentiles for numeric data
        for (int i = 0; i < _pctile.length; i++)
          _pctile[i] = sampleQuantile(_samples,DEFAULT_PERCENTILES[i]);
      } else {
        approxQuantiles(_pctile,DEFAULT_PERCENTILES);
      }
    }

    // remove the trailing NaNs
    for (int i = 0; i < _mins.length; i++) {
      if (Double.isNaN(_mins[i])) {
        _mins = Arrays.copyOf(_mins, i);
        break;
      }
    }
    for (int i = 0; i < _maxs.length; i++) {
      if (Double.isNaN(_maxs[i])) {
        _maxs = Arrays.copyOf(_maxs, i);
        break;
      }
    }
    for (int i = 0; i < _maxs.length>>>1; i++) {
      double t = _maxs[i]; _maxs[i] = _maxs[_maxs.length-1-i]; _maxs[_maxs.length-1-i] = t;
    }
    this.stats = _type==T_ENUM?new EnumStats(vec.domain().length):new NumStats(vec.mean(),vec.sigma(),_stat0._zeros,_mins,_maxs,_pctile);
    if (_type == T_ENUM) {
      this.hstart = 0;
      this.hstep = 1;
      this.hbrk = _domain;
    } else {
      this.hstart = _start;
      this.hstep  = _binsz;
      this.hbrk = new String[hcnt.length];
      for (int i = 0; i < hbrk.length; i++)
        hbrk[i] = Utils.p2d(i==0?_start:binValue(i));
    }
  }

  public Summary2(Vec vec, String name, BasicStat stat0, int max_qbins) {
    colname = name;
    _stat0 = stat0;
    _type = vec.isEnum()?2:vec.isInt()?1:0;
    _domain = vec.isEnum() ? vec.domain() : null;
    _gprows = 0;
    double sigma = Double.isNaN(vec.sigma()) ? 0 : vec.sigma();
    if ( _type != T_ENUM ) {
      _mins = MemoryManager.malloc8d((int)Math.min(vec.length(),NMAX));
      _maxs = MemoryManager.malloc8d((int)Math.min(vec.length(),NMAX));
      Arrays.fill(_mins, Double.NaN);
      Arrays.fill(_maxs, Double.NaN);
    } else {
      _mins = MemoryManager.malloc8d(Math.min(_domain.length,NMAX));
      _maxs = MemoryManager.malloc8d(Math.min(_domain.length,NMAX));
    }

    if( vec.isEnum() && _domain.length < MAX_HIST_SZ ) {
      _start = 0;
      _start2 = 0;
      _binsz = 1;
      _binsz2 = 1;
      hcnt = new long[_domain.length];
      hcnt2 = new long[_domain.length];
      hcnt2_min = new double[_domain.length];
      hcnt2_max = new double[_domain.length];
    } 
    else if (!(Double.isNaN(stat0._min2) || Double.isNaN(stat0._max2))) {
      // guard against improper parse (date type) or zero c._sigma
      long N = _stat0._len - stat0._nas - stat0._nans - stat0._pinfs - stat0._ninfs;
      double b = Math.max(1e-4,3.5 * sigma/ Math.cbrt(N));
      double d = Math.pow(10, Math.floor(Math.log10(b)));
      if (b > 20*d/3)
        d *= 10;
      else if (b > 5*d/3)
        d *= 5;

      // tweak for integers
      if (d < 1. && vec.isInt()) d = 1.;

      // Result from the dynamic bin sizing equations
      double startSuggest = d * Math.floor(stat0._min2 / d);
      double binszSuggest = d;
      int nbinSuggest = (int) Math.ceil((stat0._max2 - startSuggest)/d) + 1;
      
      // Protect against massive binning. browser doesn't need
      int BROWSER_BIN_TARGET = 100;

      //  _binsz/_start is used in the histogramming. 
      // nbin is used in the array declaration. must be big enough. 
      // the resulting nbin, could be really large number. We need to cap it. 
      // should also be obsessive and check that it's not 0 and force to 1.
      // Since nbin is implied by _binsz, ratio _binsz and recompute nbin
      int binCase = 0; // keep track in case we assert
      if ( stat0._max2==stat0._min2) {
        binszSuggest = 0; // fixed next with other 0 cases.
        _start = stat0._min2;
        binCase = 1;
      }
      // minimum 2 if min/max different
      else if ( stat0._max2!=stat0._min2 && nbinSuggest<2 ) {
        binszSuggest = (stat0._max2 - stat0._min2) / 2.0;
        _start = stat0._min2;
        binCase = 2;
      }
      else if (nbinSuggest<1 || nbinSuggest>BROWSER_BIN_TARGET ) {
        // switch to a static equation with a fixed bin count, and recompute binszSuggest
        // one more bin than necessary for the range (99 exact. causes one extra
        binszSuggest = (stat0._max2 - stat0._min2) / (BROWSER_BIN_TARGET - 1.0);
        _start = binszSuggest * Math.floor(stat0._min2 / binszSuggest);
        binCase = 3;
      }
      else {
        // align to binszSuggest boundary
        _start = binszSuggest * Math.floor(stat0._min2 / binszSuggest);
        binCase = 4;
      }

      // _binsz = 0 means min/max are equal for reals?. Just make it a little number
      // this won't show up in browser display, since bins are labelled by start value

      // Now that we know the best bin size that will fit..Floor the _binsz if integer so visible
      // histogram looks good for integers. This is our final best bin size.
      double binsz = (binszSuggest!=0) ? binszSuggest : (vec.isInt() ? 1 : 1e-13d); 
      _binsz = vec.isInt() ? Math.floor(binsz) : binsz;

      // This equation creates possibility of some of the first bins being empty
      // also: _binsz means many _binsz2 could be empty at the start if we resused _start there
      // FIX! is this okay if the dynamic range is > 2**32
      // align to bin size?
      int nbin = (int) Math.ceil((stat0._max2 - _start)/_binsz) + 1;
      

      double impliedBinEnd = _start + (nbin * _binsz);
      String assertMsg = _start+" "+_stat0._min2+" "+_stat0._max2+
        " "+impliedBinEnd+" "+_binsz+" "+nbin+" "+startSuggest+" "+nbinSuggest+" "+binCase;
      // Log.info("Summary2 bin1. "+assertMsg);
      assert _start <= _stat0._min2 : assertMsg;
      // just in case, make sure it's big enough
      assert nbin > 0: assertMsg;

      // just for double checking we're okay (nothing outside the bin rang)
      assert impliedBinEnd>=_stat0._max2 : assertMsg;


      // create a 2nd finer grained historam for quantile estimates.
      // okay if it is approx. 1000 bins (+-1)
      // update: we allow api to change max_qbins. default 1000. larger = more accuracy
      assert max_qbins > 0 && max_qbins <= 10000000 : "max_qbins must be >0 and <= 10000000";

      // okay if 1 more than max_qbins gets created
      double d2 = (stat0._max2 - stat0._min2) / max_qbins;
      // _binsz2 = 0 means min/max are equal for reals?. Just make it a little number
      // this won't show up in browser display, since bins are labelled by start value
      _binsz2 = (d2!=0) ? d2 : (vec.isInt() ? 1 : 1e-13d); 
      _start2 = stat0._min2;
      int nbin2 = (int) Math.ceil((stat0._max2 - _start2)/_binsz2) + 1;
      double impliedBinEnd2 = _start2 + (nbin2 * _binsz2);

      assertMsg = _start2+" "+_stat0._min2+" "+_stat0._max2+
        " "+impliedBinEnd2+" "+_binsz2+" "+nbin2;
      // Log.info("Summary2 bin2. "+assertMsg);
      assert _start2 <= stat0._min2 : assertMsg;
      assert nbin2 > 0 : assertMsg;
      // can't make any assertion about _start2 vs _start  (either can be smaller due to fp issues)
      assert impliedBinEnd2>=_stat0._max2 : assertMsg;

      hcnt = new long[nbin];
      hcnt2 = new long[nbin2];
      hcnt2_min = new double[nbin2];
      hcnt2_max = new double[nbin2];

      // Log.info("Finer histogram has "+nbin2+" bins. Visible histogram has "+nbin);
      // Log.info("Finer histogram starts at "+_start2+" Visible histogram starts at "+_start);
      // Log.info("stat0._min2 "+stat0._min2+" stat0._max2 "+stat0._max2);

    } else { // vec does not contain finite numbers
      Log.info("Summary2: NaN in stat0._min2: "+stat0._min2+" or stat0._max2: "+stat0._max2);
      // vec.min() wouldn't be any better here. It could be NaN? 4/13/14
      // _start = vec.min();
      // _start2 = vec.min();
      // _binsz = Double.POSITIVE_INFINITY;
      // _binsz2 = Double.POSITIVE_INFINITY;
      _start = Double.NaN;
      _start2 = Double.NaN;
      _binsz = Double.NaN;
      _binsz2 = Double.NaN;
      hcnt = new long[1];
      hcnt2 = new long[1];
      hcnt2_min = new double[1];
      hcnt2_max = new double[1];
    }
  }

  public Summary2(Vec vec, String name, BasicStat stat0) {
    this(vec, name, stat0, 1000);
  }

  /**
   * Copy non-empty elements to an array.
   * @param chk Chunk to copy from
   * @return array of non-empty elements
   */
  double[] copy1(Chunk chk) {
    double[] dbls = new double[_len1==0?128:_len1];
    double val;
    int ns = 0;
    for (int i = 0; i < chk._len; i++) if (!chk.isNA0(i))
      if (!Double.isNaN(val = chk.at0(i))) {
        if (ns == dbls.length) dbls = Arrays.copyOf(dbls,dbls.length<<1);
        dbls[ns++] = val;
      }
    if (ns < dbls.length) dbls = Arrays.copyOf(dbls,ns);
    return dbls;
  }

  // FIX! should eventually get rid of this since unused?
  public double[] resample(Chunk chk) {
    Random r = new Random(chk._start);
    if (_stat0.len1() <= RESAMPLE_SZ) return copy1(chk);
    int ns = (int)(_len1*RESAMPLE_SZ/_stat0.len1()) + 1;
    double[] dbls = new double[ns];
    if (ns<<3 < _len1 && ns<<3 < chk._len) {
      // Chunk pretty dense, sample directly
      int n = 0;
      while (n < ns) {
        double val;
        int i = r.nextInt(chk._len);
        if (chk.isNA0(i)) continue;
        if (Double.isNaN(val = chk.at0(i))) continue;
        dbls[n++] = val;
      }
      return dbls;
    }
    dbls = copy1(chk);
    if (dbls.length <= ns) return dbls;
    for (int i = dbls.length-1; i >= ns; i--)
      dbls[r.nextInt(i+1)] = dbls[i];
    return Arrays.copyOf(dbls,ns);
  }

  public Summary2 add(Chunk chk) {
    for (int i = 0; i < chk._len; i++)
      add(chk.at0(i));
    // FIX! should eventually get rid of this since unused?
    if (false) { // disabling to save mem
      _samples = resample(chk);
    }
    return this;
  }
  public void add(double val) {
    if( Double.isNaN(val) ) return;
    // can get infinity due to bad enum parse to real
    // histogram is sized ok, but the index calc below will be too big
    // just drop them. not sure if something better to do?
    if( val==Double.POSITIVE_INFINITY ) return;
    if( val==Double.NEGATIVE_INFINITY ) return;
    _len1++; _gprows++;

    if ( _type != T_ENUM ) {
      int index;
      // update min/max
      if (val < _mins[_mins.length-1] || Double.isNaN(_mins[_mins.length-1])) {
        index = Arrays.binarySearch(_mins, val);
        if (index < 0) {
          index = -(index + 1);
          for (int j = _mins.length -1; j > index; j--)
            _mins[j] = _mins[j-1];
          _mins[index] = val;
        }
      }
      boolean hasNan = Double.isNaN(_maxs[_maxs.length-1]);
      if (val > _maxs[0] || hasNan) {
        index = Arrays.binarySearch(_maxs, val);
        if (index < 0) {
          index = -(index + 1);
          if (hasNan) {
            for (int j = _maxs.length -1; j > index; j--)
              _maxs[j] = _maxs[j-1];
            _maxs[index] = val;
          } else {
            for (int j = 0; j < index-1; j++)
              _maxs[j] = _maxs[j+1];
            _maxs[index-1] = val;
          }
        }
      }
      // update the finer histogram (used for quantile estimates on numerics)
      long binIdx2;
      if (hcnt2.length==1) {
        binIdx2 = 0; // not used
      }
      else {
        binIdx2 = (int) Math.floor((val - _start2) / _binsz2);
      }

      int binIdx2Int = (int) binIdx2;
      assert (binIdx2Int >= 0 && binIdx2Int < hcnt2.length) : 
        "binIdx2Int too big for hcnt2 "+binIdx2Int+" "+hcnt2.length+" "+val+" "+_start2+" "+_binsz2;

      if (hcnt2[binIdx2Int] == 0) {
        // Log.info("New init: "+val+" for index "+binIdx2Int);
        hcnt2_min[binIdx2Int] = val;
        hcnt2_max[binIdx2Int] = val;
      }
      else {
        if (val < hcnt2_min[binIdx2Int]) {
            // Log.info("New min: "+val+" for index "+binIdx2Int);
            hcnt2_min[binIdx2Int] = val;
        }
        if (val > hcnt2_max[binIdx2Int]) {
            // if ( binIdx2Int == 500 ) Log.info("New max: "+val+" for index "+binIdx2Int);
            hcnt2_max[binIdx2Int] = val;
        }
      }
      ++hcnt2[binIdx2Int];
    }

    // update the histogram the browser/json uses
    long binIdx;
    if (hcnt.length == 1) {
      binIdx = 0;
    }
    // interesting. do we really track Infs in the histogram?
    else if (val == Double.NEGATIVE_INFINITY) {
      binIdx = 0;
    }
    else if (val == Double.POSITIVE_INFINITY) {
      binIdx = hcnt.length-1;
    }
    else {
      binIdx = (int) Math.floor((val - _start) / _binsz);
    }

    int binIdxInt = (int) binIdx;
    assert (binIdxInt >= 0 && binIdx < hcnt.length) : 
        "binIdxInt too big for hcnt2 "+binIdxInt+" "+hcnt.length+" "+val+" "+_start+" "+_binsz;
    ++hcnt[binIdxInt];
  }

  public Summary2 add(Summary2 other) {

    // merge hcnt and hcnt just by adding
    if (hcnt != null)
      Utils.add(hcnt, other.hcnt);

    _gprows += other._gprows;

    // FIX! no longer using
    // merge samples
    if (false) { // disabling to save mem
      double merged[] = new double[_samples.length+other._samples.length];
      System.arraycopy(_samples,0,merged,0,_samples.length);
      System.arraycopy(other._samples,0,merged,_samples.length,other._samples.length);
      _samples = merged;
    }
    
    if (_type == T_ENUM) return this;

    // merge hcnt2 per-bin mins 
    // other must be same length, but use it's length for safety
    // could add assert on lengths?
    for (int k = 0; k < other.hcnt2_min.length; k++) {
      // for now..die on NaNs
      assert !Double.isNaN(other.hcnt2_min[k]) : "NaN in other.hcnt2_min merging";
      assert !Double.isNaN(other.hcnt2[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2_min[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2[k]) : "NaN in hcnt2_min merging";

      // cover the initial case (relying on initial min = 0 to work is wrong)
      // Only take the new max if it's hcnt2 is non-zero. like a valid bit
      // can hcnt2 ever be null here?
      if (other.hcnt2[k] > 0) {
        if ( hcnt2[k]==0 || ( other.hcnt2_min[k] < hcnt2_min[k] )) {
          hcnt2_min[k] = other.hcnt2_min[k];
        }
      }
    }


    // merge hcnt2 per-bin maxs
    // other must be same length, but use it's length for safety
    for (int k = 0; k < other.hcnt2_max.length; k++) {
      // for now..die on NaNs
      assert !Double.isNaN(other.hcnt2_max[k]) : "NaN in other.hcnt2_max merging";
      assert !Double.isNaN(other.hcnt2[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2_max[k]) : "NaN in hcnt2_max merging";
      assert !Double.isNaN(hcnt2[k]) : "NaN in hcnt2_max merging";

      // cover the initial case (relying on initial min = 0 to work is wrong)
      // Only take the new max if it's hcnt2 is non-zero. like a valid bit
      // can hcnt2 ever be null here?
      if (other.hcnt2[k] > 0) {
        if ( hcnt2[k]==0 || ( other.hcnt2_max[k] > hcnt2_max[k] )) {
          hcnt2_max[k] = other.hcnt2_max[k];
        }
      }
    }

    // can hcnt2 ever be null here?. Inc last, so the zero case is detected above
    // seems like everything would fail if hcnt2 doesn't exist here
    if (hcnt2 != null)
      Utils.add(hcnt2, other.hcnt2);
      
    // merge hcnt mins
    double[] ds = MemoryManager.malloc8d(_mins.length);
    int i = 0, j = 0;
    for (int k = 0; k < ds.length; k++)
      if (_mins[i] < other._mins[j])
        ds[k] = _mins[i++];
      else if (Double.isNaN(other._mins[j]))
        ds[k] = _mins[i++];
      else {            // _min[i] >= other._min[j]
        if (_mins[i] == other._mins[j]) i++;
        ds[k] = other._mins[j++];
      }
    System.arraycopy(ds,0,_mins,0,ds.length);

    for (i = _maxs.length - 1; Double.isNaN(_maxs[i]); i--) if (i == 0) {i--; break;}
    for (j = _maxs.length - 1; Double.isNaN(other._maxs[j]); j--) if (j == 0) {j--; break;}

    ds = MemoryManager.malloc8d(i + j + 2);
    // merge hcnt maxs, also deduplicating against mins?
    int k = 0, ii = 0, jj = 0;
    while (ii <= i && jj <= j) {
      if (_maxs[ii] < other._maxs[jj])
        ds[k] = _maxs[ii++];
      else if (_maxs[ii] > other._maxs[jj])
        ds[k] = other._maxs[jj++];
      else { // _maxs[ii] == other.maxs[jj]
        ds[k] = _maxs[ii++];
        jj++;
      }
      k++;
    }
    while (ii <= i) ds[k++] = _maxs[ii++];
    while (jj <= j) ds[k++] = other._maxs[jj++];
    System.arraycopy(ds,Math.max(0, k - _maxs.length),_maxs,0,Math.min(k,_maxs.length));
    for (int t = k; t < _maxs.length; t++) _maxs[t] = Double.NaN;
    return this;
  }

  // _start of each hcnt bin
  public double binValue(int b) { return _start + b*_binsz; }

  // FIX! should eventually get rid of this since unused
  private double sampleQuantile(final double[] samples, final double threshold) {
    assert 0.0 <= threshold && threshold <= 1.0;
    int ix = (int)(samples.length * threshold);
    return ix<samples.length?samples[ix]:Double.NaN;
  }

  // need to count >4B rows
  private long htot2() { // same but for the finer histogram
    long cnt = 0;
    for (int i = 0; i < hcnt2.length; i++) cnt+=hcnt2[i];
    return cnt;
  }

  private void approxQuantiles(double[] qtiles, double[] thres){
    // not called for enums
    assert _type != T_ENUM;
    if ( hcnt2.length==0 ) return;
    // this would imply we didn't get anything correctly. Maybe real col with all NA?
    if ( (hcnt2.length==1) && (hcnt2[0]==0) )  return;

    int k = 0;
    long s = 0;
    double guess = 0;
    double actualBinWidth = 0;
    assert _gprows==htot2() : "_gprows: "+_gprows+" htot2(): "+htot2();

    // One goal definition: (Excel?)
    // Given a set of N ordered values {v[1], v[2], ...} and a requirement to 
    // calculate the pth percentile, do the following:
    // Calculate l = p(N-1) + 1
    // Split l into integer and decimal components i.e. l = k + d
    // Compute the required value as V = v[k] + d(v[k+1] - v[k])

    // walk up until we're at the bin that starts with the threshold, or right before
    for(int j = 0; j < thres.length; ++j) {
      // 0 okay for threshold?
      assert 0 <= thres[j] && thres[j] <= 1;
      double s1 = Math.floor(thres[j] * (double) _gprows); 
      if ( s1 == 0 ) {
        s1 = 1; // always need at least one row
      }
      // what if _gprows is 0?. just return above?. Is it NAs?
      // assert _gprows > 0 : _gprows;
      if( _gprows == 0 ) return;
      assert 1 <= s1 && s1 <= _gprows : s1+" "+_gprows;
      // how come first bins can be 0? Fixed. problem was _start. Needed _start2. still can get some
      while( (s+hcnt2[k]) < s1) { // important to be < here. case: 100 rows, getting 50% right.
        s += hcnt2[k];
        k++;
      }
      // Log.info("Found k: "+k+" "+s+" "+s1+" "+_gprows+" "+hcnt2[k]+" "+hcnt2_min[k]+" "+hcnt2_max[k]);

      // All possible bin boundary issues 
      if ( s==s1 || hcnt2[k]==0 ) {
        if ( hcnt2[k]!=0 ) {
          guess = hcnt2_min[k];
          // Log.info("Guess A: "+guess+" "+s+" "+s1);
        }
        else {
          if ( k==0 ) { 
            assert hcnt2[k+1]!=0 : "Unexpected state of starting hcnt2 bins";
            guess = hcnt2_min[k+1];
            // Log.info("Guess B: "+guess+" "+s+" "+s1);
          }
          else {
            if ( hcnt2[k-1]!=0 ) {
              guess = hcnt2_max[k-1];
              // Log.info("Guess C: "+guess+" "+s+" "+s1);
            }
            else {
              assert false : "Unexpected state of adjacent hcnt2 bins";
            }
          }
        }
      }
      else {
        // nonzero hcnt2[k] guarantees these are valid
        actualBinWidth = hcnt2_max[k] - hcnt2_min[k];

        // interpolate within the populated bin, assuming linear distribution
        // since we have the actual min/max within a bin, we can be more accurate
        // compared to using the bin boundaries
        // Note actualBinWidth is 0 when all values are the same in a bin
        // Interesting how we have a gap that we jump between max of one bin, and min of another.
        guess = hcnt2_min[k] + actualBinWidth * ((s1 - s)/ hcnt2[k]);
        // Log.info("Guess D: "+guess+" "+k+" "+hcnt2_min[k]+" "+actualBinWidth+" "+s+" "+s1+" "+hcnt2[k]);
      }

      qtiles[j] = guess;

      // _maxs[5] is usually the biggest (not always)?  _mins[0] is the smallest
      // oh..ugly. At this point, NaNs haven't been stripped. so we don't know that 
      // the end of _maxs has the true max (if there is just 1-4 values in the data, 
      // _maxs doens't get filled (legacy!). The NaNs and array length get flushed later.
      // _maxs really should have been organized as big to small so biggest is always in 0.
      // So find the last max before the nans
      double trueMax = _maxs[0];
      for(int p = 1; p < _maxs.length; ++p) {
        if ( !Double.isNaN(_maxs[p]) ) trueMax = _maxs[p];
      }

      // might have fp tolerance issues here? but fp numbers should be exactly same?
      // assert guess <= trueMax : guess+" "+trueMax;
      // assert guess >= _mins[0] : guess+" "+_mins[0];
      // Log.info("_mins[0]: "+_mins[0]+" trueMax: "+trueMax+" hcnt2[k]: "+hcnt2[k]+" hcnt2_min[k]: "+hcnt2_min[k]+
      // " hcnt2_max[k]: "+hcnt2_max[k]+" _binsz2: "+_binsz2+" guess: "+guess+" k: "+k+"\n");
    }
  }
  // Compute majority categories for enums only
  public void computeMajorities() {
    if ( _type != T_ENUM ) return;
    for (int i = 0; i < _mins.length; i++) _mins[i] = i;
    for (int i = 0; i < _maxs.length; i++) _maxs[i] = i;
    int mini = 0, maxi = 0;
    for( int i = 0; i < hcnt.length; i++ ) {
      if (hcnt[i] < hcnt[(int)_mins[mini]]) {
        _mins[mini] = i;
        for (int j = 0; j < _mins.length; j++)
          if (hcnt[(int)_mins[j]] > hcnt[(int)_mins[mini]]) mini = j;
      }
      if (hcnt[i] > hcnt[(int)_maxs[maxi]]) {
        _maxs[maxi] = i;
        for (int j = 0; j < _maxs.length; j++)
          if (hcnt[(int)_maxs[j]] < hcnt[(int)_maxs[maxi]]) maxi = j;
      }
    }
    for (int i = 0; i < _mins.length - 1; i++)
      for (int j = 0; j < i; j++) {
        if (hcnt[(int)_mins[j]] > hcnt[(int)_mins[j+1]]) {
          double t = _mins[j]; _mins[j] = _mins[j+1]; _mins[j+1] = t;
        }
      }
    for (int i = 0; i < _maxs.length - 1; i++)
      for (int j = 0; j < i; j++)
        if (hcnt[(int)_maxs[j]] < hcnt[(int)_maxs[j+1]]) {
          double t = _maxs[j]; _maxs[j] = _maxs[j+1]; _maxs[j+1] = t;
        }
  }

  public double percentileValue(int idx) {
    if( _type == T_ENUM ) return Double.NaN;
     return _pctile[idx];
  }

  public void toHTML( Vec vec, String cname, StringBuilder sb ) {
    // should be a better way/place to decode this back to string.
    String typeStr;
    if ( _type == T_REAL) typeStr = "Real";
    else if ( _type == T_INT) typeStr = "Int";
    else if ( _type == T_ENUM) typeStr = "Enum";
    else typeStr = "Undefined";

    sb.append("<div class='table' id='col_" + cname + "' style='width:90%;heigth:90%;border-top-style:solid;'>" +
    "<div class='alert-success'><h4>Column: " + cname + " (type: " + typeStr + ")</h4></div>\n");
    if ( _stat0._len == _stat0._nas ) {
      sb.append("<div class='alert'>Empty column, no summary!</div></div>\n");
      return;
    }
    // Base stats
    if( _type != T_ENUM ) {
      NumStats stats = (NumStats)this.stats;
      sb.append("<div style='width:100%;'><table class='table-bordered'>");
      sb.append("<tr><th colspan='"+20+"' style='text-align:center;'>Base Stats</th></tr>");
      sb.append("<tr>");
      sb.append("<th>NAs</th>  <td>" + nacnt + "</td>");
      sb.append("<th>mean</th><td>" + Utils.p2d(stats.mean)+"</td>");
      sb.append("<th>sd</th><td>" + Utils.p2d(stats.sd) + "</td>");
      sb.append("<th>zeros</th><td>" + stats.zeros + "</td>");
      sb.append("<th>min[" + stats.mins.length + "]</th>");
      for( double min : stats.mins ) {
        sb.append("<td>" + Utils.p2d(min) + "</td>");
      }
      sb.append("<th>max[" + stats.maxs.length + "]</th>");
      for( double max : stats.maxs ) {
        sb.append("<td>" + Utils.p2d(max) + "</td>");
      }
      // End of base stats
      sb.append("</tr> </table>");
      sb.append("</div>");
    } else {                    // Enums
      sb.append("<div style='width:100%'><table class='table-bordered'>");
      sb.append("<tr><th colspan='" + 4 + "' style='text-align:center;'>Base Stats</th></tr>");
      sb.append("<tr><th>NAs</th>  <td>" + nacnt + "</td>");
      sb.append("<th>cardinality</th>  <td>" + vec.domain().length + "</td></tr>");
      sb.append("</table></div>");
    }
    // Histogram
    final int MAX_HISTO_BINS_DISPLAYED = 1000;
    int len = Math.min(hcnt.length,MAX_HISTO_BINS_DISPLAYED);
    sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
    sb.append("<tr> <th colspan="+len+" style='text-align:center'>Histogram</th></tr>");
    sb.append("<tr>");
    if ( _type == T_ENUM )
       for( int i=0; i<len; i++ ) sb.append("<th>" + vec.domain(i) + "</th>");
    else
       for( int i=0; i<len; i++ ) sb.append("<th>" + Utils.p2d(i==0?_start:binValue(i)) + "</th>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ ) sb.append("<td>" + hcnt[i] + "</td>");
    sb.append("</tr>");
    sb.append("<tr>");
    for( int i=0; i<len; i++ )
      sb.append(String.format("<td>%.1f%%</td>",(100.0*hcnt[i]/_stat0._len)));
    sb.append("</tr>");
    if( hcnt.length >= MAX_HISTO_BINS_DISPLAYED )
      sb.append("<div class='alert'>Histogram for this column was too big and was truncated to 1000 values!</div>");
    sb.append("</table></div>");

    if (_type != T_ENUM) {
      NumStats stats = (NumStats)this.stats;
      // Percentiles
      sb.append("<div style='width:100%;overflow-x:auto;'><table class='table-bordered'>");
      sb.append("<tr> <th colspan='" + stats.pct.length + "' " +
              "style='text-align:center' " +
              ">Percentiles</th></tr>");
      sb.append("<tr><th>Threshold(%)</th>");
      for (double pc : stats.pct)
        sb.append("<td>" + Utils.p2d(pc * 100.0) + "</td>");
        // sb.append("<td>" + (int) Math.round(pc * 100) + "</td>");
      sb.append("</tr>");
      sb.append("<tr><th>Value</th>");
      for (double pv : stats.pctile)
        sb.append("<td>" + Utils.p2d(pv) + "</td>");
      sb.append("</tr>");
      sb.append("</table>");
      sb.append("</div>");
    }
    sb.append("</div>\n"); 
  }
}
