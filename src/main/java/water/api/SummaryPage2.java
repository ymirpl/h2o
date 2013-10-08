package water.api;

import hex.Summary2;
import water.*;
import water.util.RString;
import water.fvec.*;
import water.util.Utils;

import java.util.Iterator;

/**
 *
 */
public class SummaryPage2 extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a summary of a fluid-vec frame";

  @API(help="An existing H2O Frame key.", required=true, filter=Default.class)
  Frame source;

  class colsFilter1 extends MultiVecSelect { public colsFilter1() { super("source");} }
  @API(help = "Select columns", required=true,  filter=colsFilter1.class)
  int[] cols;

  @API(help = "Maximum columns to show summaries of", required = true,
          filter = Default.class, lmin = 1,  lmax = 1000)
  int max_ncols = 1000;

  @API(help = "Column summaries.")
  Summary2[] summaries;

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='SummaryPage2.query?source=%$key'>"+content+"</a>");
    rs.replace("key", k.toString());
    return rs.toString();
  }

  @Override protected Response serve() {
    if( source == null ) return RequestServer._http404.serve();
    String[] names = new String[cols.length];
    Vec[] vecs = new Vec[cols.length];
    for (int i = 0; i < cols.length; i++) {
      vecs[i] = source.vecs()[cols[i]];
      names[i] = source._names[cols[i]];
    }
    Frame fr = new Frame(names, vecs);
    summaries = new SummaryTask2().doAll(fr)._summaries;
    for( Summary2 s2 : summaries ) s2.percentileValue(0);
    return new Response(Response.Status.done, this, -1, -1, null);
  }

  private static class SummaryTask2 extends MRTask2<SummaryTask2> {
    Summary2 _summaries[];
    @Override public void map(Chunk[] cs) {
      _summaries = new Summary2[cs.length];
      for (int i = 0; i < cs.length; i++) {
        (_summaries[i]=new Summary2(cs[i]._vec)).add(cs[i]);
      }
    }
    @Override public void reduce(SummaryTask2 other) {
      for (int i = 0; i < _summaries.length; i++)
        _summaries[i].add(other._summaries[i]);
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    for( int i = 0; i < summaries.length; i++) {
      String cname = source._names[cols[i]];
      Summary2 s2 = summaries[i];
      s2.toHTML(source.vecs()[cols[i]],cname,sb);
    }
    return true;
  }
}