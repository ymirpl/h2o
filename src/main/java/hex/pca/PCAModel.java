package hex.pca;

import hex.FrameTask.DataInfo;
import hex.gram.Gram.GramTask;
import water.*;
import water.api.*;
import water.api.Request.API;
import water.api.RequestBuilders.ElementBuilder;
import water.fvec.Frame;
import water.fvec.Vec;

public class PCAModel extends Model {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Column names expanded to accommodate categoricals")
  final String[] namesExp;

  @API(help = "Standard deviation of each principal component")
  final double[] sdev;

  @API(help = "Proportion of variance explained by each principal component")
  final double[] propVar;

  @API(help = "Cumulative proportion of variance explained by each principal component")
  final double[] cumVar;

  @API(help = "Principal components (eigenvector) matrix")
  final double[][] eigVec;

  @API(help = "If standardized, mean of each numeric data column")
  final double[] normSub;

  @API(help = "If standardized, one over standard deviation of each numeric data column")
  final double[] normMul;

  @API(help = "Offsets of categorical columns into the sdev vector. The last value is the offset of the first numerical column.")
  final int[] catOffsets;

  @API(help = "Rank of eigenvector matrix")
  final int rank;

  @API(help = "Number of principal components to display")
  int num_pc;

  @API(help = "PCA parameters")
  final PCAParams params;

  public PCAModel(Key selfKey, Key dataKey, DataInfo dinfo, GramTask gramt, double[] sdev, double[] propVar, double[] cumVar, double[][] eigVec, int rank, int num_pc, PCAParams params) {
    super(selfKey, dataKey, dinfo._adaptedFrame);
    this.sdev = sdev;
    this.propVar = propVar;
    this.cumVar = cumVar;
    this.eigVec = eigVec;
    this.params = params;
    this.catOffsets = dinfo._catOffsets;
    this.namesExp = namesExp();
    this.rank = rank;
    this.num_pc = num_pc;

    // TODO: Need to ensure this maps correctly to scored data cols
    this.normSub = gramt.normSub();
    this.normMul = gramt.normMul();
  }

  public double[] sdev() { return sdev; }
  public double[][] eigVec() { return eigVec; }

  @Override protected float[] score0(double[] data, float[] preds) {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

  @Override public void delete() { super.delete(); }

  @Override public String toString(){
    StringBuilder sb = new StringBuilder("PCA Model (key=" + _key + " , trained on " + _dataKey + "):\n");
    return sb.toString();
  }

  public String[] namesExp(){
    final int n = _names.length;
    int[] nums = MemoryManager.malloc4(n);
    int[] cats = MemoryManager.malloc4(n);

    // Store indices of numeric and categorical cols
    int nnums = 0, ncats = 0;
    for(int i = 0; i < n; ++i){
      if(_domains[i] != null)
        cats[ncats++] = i;
      else
        nums[nnums++] = i;
    }

    // Sort the categoricals in decreasing order according to size
    for(int i = 0; i < ncats; ++i)
      for(int j = i+1; j < ncats; ++j)
        if(_domains[cats[i]].length < _domains[cats[j]].length) {
          int x = cats[i];
          cats[i] = cats[j];
          cats[j] = x;
        }

    // Construct expanded col names, with categoricals first followed by numerics
    int k = 0;
    String[] names = new String[sdev.length];
    for(int i = 0; i < ncats; ++i){
      for(int j = 1; j < _domains[cats[i]].length; ++j)
        names[k++] = _names[cats[i]] + "." + _domains[cats[i]][j];
    }
    for(int i = 0; i < nnums; ++i) {
      names[k++] = _names[nums[i]];
    }
    return names;
  }

  public void generateHTML(String title, StringBuilder sb) {
    if(title != null && !title.isEmpty()) DocGen.HTML.title(sb, title);
    DocGen.HTML.paragraph(sb, "Model Key: " + _key);

    sb.append("<script type=\"text/javascript\" src='/h2o/js/d3.v3.min.js'></script>");
    sb.append("<div class='alert'>Actions: " + PCAScore.link(_key, "Score on dataset") + ", "
        + PCA.link(_dataKey, "Compute new model") + "</div>");
    screevarString(sb);
    sb.append("<span style='display: inline-block;'>");
    sb.append("<table class='table table-striped table-bordered'>");
    sb.append("<tr>");
    sb.append("<th>Feature</th>");

    for(int i = 0; i < num_pc; i++)
      sb.append("<th>").append("PC" + i).append("</th>");
    sb.append("</tr>");

    // Row of standard deviation values
    sb.append("<tr class='warning'>");
    // sb.append("<td>").append("&sigma;").append("</td>");
    sb.append("<td>").append("Std Dev").append("</td>");
    for(int c = 0; c < num_pc; c++)
      sb.append("<td>").append(ElementBuilder.format(sdev[c])).append("</td>");
    sb.append("</tr>");

    // Row with proportion of variance
    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Prop Var").append("</td>");
    for(int c = 0; c < num_pc; c++)
      sb.append("<td>").append(ElementBuilder.format(propVar[c])).append("</td>");
    sb.append("</tr>");

    // Row with cumulative proportion of variance
    sb.append("<tr class='warning'>");
    sb.append("<td>").append("Cum Prop Var").append("</td>");
    for(int c = 0; c < num_pc; c++)
      sb.append("<td>").append(ElementBuilder.format(cumVar[c])).append("</td>");
    sb.append("</tr>");

    // Each row is component of eigenvector
    for(int r = 0; r < eigVec.length; r++) {
      sb.append("<tr>");
      sb.append("<th>").append(namesExp[r]).append("</th>");
      for( int c = 0; c < num_pc; c++ ) {
        double e = eigVec[r][c];
        sb.append("<td>").append(ElementBuilder.format(e)).append("</td>");
      }
      sb.append("</tr>");
    }
    sb.append("</table></span>");
  }

  public void screevarString(StringBuilder sb) {
    sb.append("<div class=\"pull-left\"><a href=\"#\" onclick=\'$(\"#scree_var\").toggleClass(\"hide\");\' class=\'btn btn-inverse btn-mini\'>Scree & Variance Plots</a></div>");
    sb.append("<div class=\"hide\" id=\"scree_var\">");
    sb.append("<style type=\"text/css\">");
    sb.append(".axis path," +
    ".axis line {\n" +
    "fill: none;\n" +
      "stroke: black;\n" +
      "shape-rendering: crispEdges;\n" +
    "}\n" +

    ".axis text {\n" +
      "font-family: sans-serif;\n" +
      "font-size: 11px;\n" +
    "}\n");

    sb.append("</style>");
    sb.append("<div id=\"scree\" style=\"display:inline;\">");
    sb.append("<script type=\"text/javascript\">");

    sb.append("//Width and height\n");
    sb.append("var w = 500;\n"+
    "var h = 300;\n"+
    "var padding = 40;\n"
    );
    sb.append("var dataset = [");

    for(int c = 0; c < num_pc; c++) {
      if (c == 0) {
        sb.append("["+String.valueOf(c+1)+",").append(ElementBuilder.format(sdev[c]*sdev[c])).append("]");
      }
      sb.append(", ["+String.valueOf(c+1)+",").append(ElementBuilder.format(sdev[c]*sdev[c])).append("]");
    }
    sb.append("];");

    sb.append(
    "//Create scale functions\n"+
    "var xScale = d3.scale.linear()\n"+
               ".domain([0, d3.max(dataset, function(d) { return d[0]; })])\n"+
               ".range([padding, w - padding * 2]);\n"+

    "var yScale = d3.scale.linear()"+
               ".domain([0, d3.max(dataset, function(d) { return d[1]; })])\n"+
               ".range([h - padding, padding]);\n"+

    "var rScale = d3.scale.linear()"+
               ".domain([0, d3.max(dataset, function(d) { return d[1]; })])\n"+
               ".range([2, 5]);\n"+

    "//Define X axis\n"+
    "var xAxis = d3.svg.axis()\n"+
              ".scale(xScale)\n"+
              ".orient(\"bottom\")\n"+
              ".ticks(5);\n"+

    "//Define Y axis\n"+
    "var yAxis = d3.svg.axis()\n"+
              ".scale(yScale)\n"+
              ".orient(\"left\")\n"+
              ".ticks(5);\n"+

    "//Create SVG element\n"+
    "var svg = d3.select(\"#scree\")\n"+
          ".append(\"svg\")\n"+
          ".attr(\"width\", w)\n"+
          ".attr(\"height\", h);\n"+

    "//Create circles\n"+
    "svg.selectAll(\"circle\")\n"+
       ".data(dataset)\n"+
       ".enter()\n"+
       ".append(\"circle\")\n"+
       ".attr(\"cx\", function(d) {\n"+
          "return xScale(d[0]);\n"+
       "})\n"+
       ".attr(\"cy\", function(d) {\n"+
          "return yScale(d[1]);\n"+
       "})\n"+
       ".attr(\"r\", function(d) {\n"+
          "return 2;\n"+//rScale(d[1]);\n"+
       "});\n"+

    "/*"+
    "//Create labels\n"+
    "svg.selectAll(\"text\")"+
       ".data(dataset)"+
       ".enter()"+
       ".append(\"text\")"+
       ".text(function(d) {"+
          "return d[0] + \",\" + d[1];"+
       "})"+
       ".attr(\"x\", function(d) {"+
          "return xScale(d[0]);"+
       "})"+
       ".attr(\"y\", function(d) {"+
          "return yScale(d[1]);"+
       "})"+
       ".attr(\"font-family\", \"sans-serif\")"+
       ".attr(\"font-size\", \"11px\")"+
       ".attr(\"fill\", \"red\");"+
      "*/\n"+

    "//Create X axis\n"+
    "svg.append(\"g\")"+
      ".attr(\"class\", \"axis\")"+
      ".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
      ".call(xAxis);\n"+

    "//X axis label\n"+
    "d3.select('#scree svg')"+
      ".append(\"text\")"+
      ".attr(\"x\",w/2)"+
      ".attr(\"y\",h - 5)"+
      ".attr(\"text-anchor\", \"middle\")"+
      ".text(\"Principal Component\");\n"+

    "//Create Y axis\n"+
    "svg.append(\"g\")"+
      ".attr(\"class\", \"axis\")"+
      ".attr(\"transform\", \"translate(\" + padding + \",0)\")"+
      ".call(yAxis);\n"+

    "//Y axis label\n"+
    "d3.select('#scree svg')"+
      ".append(\"text\")"+
      ".attr(\"x\",150)"+
      ".attr(\"y\",-5)"+
      ".attr(\"transform\", \"rotate(90)\")"+
      //".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
      ".attr(\"text-anchor\", \"middle\")"+
      ".text(\"Eigenvalue\");\n"+

    "//Title\n"+
    "d3.select('#scree svg')"+
      ".append(\"text\")"+
      ".attr(\"x\",w/2)"+
      ".attr(\"y\",padding - 20)"+
      ".attr(\"text-anchor\", \"middle\")"+
      ".text(\"Scree Plot\");\n");

    sb.append("</script>");
    sb.append("</div>");
    ///////////////////////////////////
    sb.append("<div id=\"var\" style=\"display:inline;\">");
    sb.append("<script type=\"text/javascript\">");

    sb.append("//Width and height\n");
    sb.append("var w = 500;\n"+
    "var h = 300;\n"+
    "var padding = 50;\n"
    );
    sb.append("var dataset = [");

    for(int c = 0; c < num_pc; c++) {
      if (c == 0) {
        sb.append("["+String.valueOf(c+1)+",").append(ElementBuilder.format(cumVar[c])).append("]");
      }
      sb.append(", ["+String.valueOf(c+1)+",").append(ElementBuilder.format(cumVar[c])).append("]");
    }
    sb.append("];");

    sb.append(
    "//Create scale functions\n"+
    "var xScale = d3.scale.linear()\n"+
               ".domain([0, d3.max(dataset, function(d) { return d[0]; })])\n"+
               ".range([padding, w - padding * 2]);\n"+

    "var yScale = d3.scale.linear()"+
               ".domain([0, d3.max(dataset, function(d) { return d[1]; })])\n"+
               ".range([h - padding, padding]);\n"+

    "var rScale = d3.scale.linear()"+
               ".domain([0, d3.max(dataset, function(d) { return d[1]; })])\n"+
               ".range([2, 5]);\n"+

    "//Define X axis\n"+
    "var xAxis = d3.svg.axis()\n"+
              ".scale(xScale)\n"+
              ".orient(\"bottom\")\n"+
              ".ticks(5);\n"+

    "//Define Y axis\n"+
    "var yAxis = d3.svg.axis()\n"+
              ".scale(yScale)\n"+
              ".orient(\"left\")\n"+
              ".ticks(5);\n"+

    "//Create SVG element\n"+
    "var svg = d3.select(\"#var\")\n"+
          ".append(\"svg\")\n"+
          ".attr(\"width\", w)\n"+
          ".attr(\"height\", h);\n"+

    "//Create circles\n"+
    "svg.selectAll(\"circle\")\n"+
       ".data(dataset)\n"+
       ".enter()\n"+
       ".append(\"circle\")\n"+
       ".attr(\"cx\", function(d) {\n"+
          "return xScale(d[0]);\n"+
       "})\n"+
       ".attr(\"cy\", function(d) {\n"+
          "return yScale(d[1]);\n"+
       "})\n"+
       ".attr(\"r\", function(d) {\n"+
          "return 2;\n"+//rScale(d[1]);\n"+
       "});\n"+

    "/*"+
    "//Create labels\n"+
    "svg.selectAll(\"text\")"+
       ".data(dataset)"+
       ".enter()"+
       ".append(\"text\")"+
       ".text(function(d) {"+
          "return d[0] + \",\" + d[1];"+
       "})"+
       ".attr(\"x\", function(d) {"+
          "return xScale(d[0]);"+
       "})"+
       ".attr(\"y\", function(d) {"+
          "return yScale(d[1]);"+
       "})"+
       ".attr(\"font-family\", \"sans-serif\")"+
       ".attr(\"font-size\", \"11px\")"+
       ".attr(\"fill\", \"red\");"+
      "*/\n"+

    "//Create X axis\n"+
    "svg.append(\"g\")"+
      ".attr(\"class\", \"axis\")"+
      ".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
      ".call(xAxis);\n"+

    "//X axis label\n"+
    "d3.select('#var svg')"+
      ".append(\"text\")"+
      ".attr(\"x\",w/2)"+
      ".attr(\"y\",h - 5)"+
      ".attr(\"text-anchor\", \"middle\")"+
      ".text(\"Principal Component\");\n"+

    "//Create Y axis\n"+
    "svg.append(\"g\")"+
      ".attr(\"class\", \"axis\")"+
      ".attr(\"transform\", \"translate(\" + padding + \",0)\")"+
      ".call(yAxis);\n"+

    "//Y axis label\n"+
    "d3.select('#var svg')"+
      ".append(\"text\")"+
      ".attr(\"x\",150)"+
      ".attr(\"y\",-5)"+
      ".attr(\"transform\", \"rotate(90)\")"+
      //".attr(\"transform\", \"translate(0,\" + (h - padding) + \")\")"+
      ".attr(\"text-anchor\", \"middle\")"+
      ".text(\"Cumulative Proportion of Variance\");\n"+

    "//Title\n"+
    "d3.select('#var svg')"+
      ".append(\"text\")"+
      ".attr(\"x\",w/2)"+
      ".attr(\"y\",padding-20)"+
      ".attr(\"text-anchor\", \"middle\")"+
      ".text(\"Cumulative Variance Plot\");\n");

    sb.append("</script>");
    sb.append("</div>");
    sb.append("</div>");
    sb.append("<br />");
  }
}
