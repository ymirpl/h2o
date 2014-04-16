package water.api;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

import hex.GridSearch.GridSearchProgress;
import hex.KMeans2;
import hex.KMeans2.KMeans2ModelView;
import hex.KMeans2.KMeans2Progress;
import hex.NeuralNet;
import hex.NeuralNet.NeuralNetScore;
import hex.drf.DRF;
import hex.gbm.GBM;
import hex.glm.GLM2;
import hex.glm.GLMGridView;
import hex.glm.GLMModelView;
import hex.glm.GLMProgress;
import hex.deeplearning.DeepLearning;
import hex.pca.PCA;
import hex.pca.PCAModelView;
import hex.pca.PCAProgressPage;
import hex.pca.PCAScore;
import water.Boot;
import water.H2O;
import water.NanoHTTPD;
import water.api.Script.RunScript;
import water.api.Upload.PostFile;
import water.api.v2.*;
import water.deploy.LaunchJar;
import water.util.Log;
import water.util.Log.Tag.Sys;
import water.util.Utils.ExpectedExceptionForDebug;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This is a simple web server. */
public class RequestServer extends NanoHTTPD {
  public enum API_VERSION {
    V_1(1, "/"),
    V_2(2, "/2/"), // FIXME: better should be /v2/
    V_v2(3, "/v2/");
    final private int _version;
    final private String _prefix;
    public final String prefix() { return _prefix; }
    private API_VERSION(int version, String prefix) { _version = version; _prefix = prefix; }
  }
  static RequestServer SERVER;

  // cache of all loaded resources
  private static final ConcurrentHashMap<String,byte[]> _cache = new ConcurrentHashMap();
  protected static final HashMap<String,Request> _requests = new HashMap();

  static final Request _http404;
  static final Request _http500;

  // initialization ------------------------------------------------------------
  static {
    boolean USE_NEW_TAB = true;

    _http404 = registerRequest(new HTTP404());
    _http500 = registerRequest(new HTTP500());

    Request.addToNavbar(registerRequest(new Inspect()),     "Inspect",                    "Data");
    Request.addToNavbar(registerRequest(new StoreView()),   "View All",                   "Data");
    Request.addToNavbar(registerRequest(new Parse()),       "Parse",                      "Data");
    Request.addToNavbar(registerRequest(new ImportFiles()), "Import Files",               "Data");
    Request.addToNavbar(registerRequest(new ImportUrl()),   "Import URL",                 "Data");
    Request.addToNavbar(registerRequest(new ImportS3()),    "Import S3",                  "Data");
    Request.addToNavbar(registerRequest(new ExportS3()),    "Export S3",                  "Data");
    Request.addToNavbar(registerRequest(new ImportHdfs()),  "Import HDFS",                "Data");
    Request.addToNavbar(registerRequest(new ExportHdfs()),  "Export HDFS",                "Data");
    Request.addToNavbar(registerRequest(new Upload()),      "Upload",                     "Data");

    Request.addToNavbar(registerRequest(new SummaryPage()), "Summary",                    "Model");
    Request.addToNavbar(registerRequest(new GLM()),         "GLM",                        "Model");
    Request.addToNavbar(registerRequest(new GLMGrid()),     "GLM Grid",                   "Model");
    Request.addToNavbar(registerRequest(new PCA()),         "PCA",                        "Model");
    Request.addToNavbar(registerRequest(new KMeans()),      "KMeans",                     "Model");
    Request.addToNavbar(registerRequest(new GBM()),         "GBM",                        "Model");
    Request.addToNavbar(registerRequest(new RF()),          "Single Node RF",             "Model");
    Request.addToNavbar(registerRequest(new DRF()),         "Distributed RF (Beta)",      "Model");
    Request.addToNavbar(registerRequest(new GLM2()),        "GLM2 (Beta)",                "Model");
    Request.addToNavbar(registerRequest(new KMeans2()),     "KMeans2 (Beta)",             "Model");
    Request.addToNavbar(registerRequest(new NeuralNet()),   "Neural Network (deprecated)","Model");
    Request.addToNavbar(registerRequest(new DeepLearning()),          "Deep Learning (Beta)",       "Model");

    Request.addToNavbar(registerRequest(new RFScore()),     "Random Forest",              "Score");
    Request.addToNavbar(registerRequest(new GLMScore()),    "GLM",                        "Score");
    Request.addToNavbar(registerRequest(new KMeansScore()), "KMeans",                     "Score");
    Request.addToNavbar(registerRequest(new KMeansApply()), "KMeans Apply",               "Score");
    Request.addToNavbar(registerRequest(new PCAScore()),    "PCA (Beta)",                 "Score");
    Request.addToNavbar(registerRequest(new NeuralNetScore()), "Neural Network (deprecated)","Score");
    Request.addToNavbar(registerRequest(new GeneratePredictionsPage()),  "Predict",       "Score");
    Request.addToNavbar(registerRequest(new Predict()),     "Predict2",                   "Score");
    Request.addToNavbar(registerRequest(new Score()),       "Apply Model",                "Score");
    Request.addToNavbar(registerRequest(new ConfusionMatrix()), "Confusion Matrix",       "Score");
    Request.addToNavbar(registerRequest(new AUC()),         "AUC",                        "Score");

    Request.addToNavbar(registerRequest(new Jobs()),        "Jobs",            "Admin");
    Request.addToNavbar(registerRequest(new Cloud()),       "Cluster Status",  "Admin");
    Request.addToNavbar(registerRequest(new IOStatus()),    "Cluster I/O",     "Admin");
    Request.addToNavbar(registerRequest(new Timeline()),    "Timeline",        "Admin");
    Request.addToNavbar(registerRequest(new JStack()),      "Stack Dump",      "Admin");
    Request.addToNavbar(registerRequest(new Debug()),       "Debug Dump",      "Admin");
    Request.addToNavbar(registerRequest(new LogView()),     "Inspect Log",     "Admin");
    Request.addToNavbar(registerRequest(new Script()),      "Get Script",      "Admin");
    Request.addToNavbar(registerRequest(new Shutdown()),    "Shutdown",        "Admin");

    Request.addToNavbar(registerRequest(new Documentation()),       "H2O Documentation",      "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new Tutorials()),           "Tutorials Home",         "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new TutorialRFIris()),      "Random Forest Tutorial", "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new TutorialGLMProstate()), "GLM Tutorial",           "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new TutorialKMeans()),      "KMeans Tutorial",        "Help", USE_NEW_TAB);
    Request.addToNavbar(registerRequest(new AboutH2O()),            "About H2O",              "Help");

    // Beta things should be reachable by the API and web redirects, but not put in the menu.
    if(H2O.OPT_ARGS.beta == null) {
      registerRequest(new ImportFiles2());
      registerRequest(new Parse2());
      registerRequest(new Inspect2());
      registerRequest(new SummaryPage2());
      registerRequest(new QuantilesPage());
      registerRequest(new hex.LR2());
    } else {
      Request.addToNavbar(registerRequest(new ImportFiles2()),   "Import Files2",        "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Parse2()),         "Parse2",               "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Upload2()),        "Upload2",              "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Inspect2()),       "Inspect2",             "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new hex.LR2()),        "Linear Regression2",   "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new SummaryPage2()),   "Summary2",             "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new QuantilesPage()),  "Quantiles",            "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new Console()),        "Console",              "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new ExportModel()),    "Export Model",         "Beta (FluidVecs!)");
      Request.addToNavbar(registerRequest(new ImportModel()),    "Import Model",         "Beta (FluidVecs!)");
    }
    registerRequest(new Get()); // Download
    //Column Expand
    registerRequest(new OneHot());
    // internal handlers
    //registerRequest(new StaticHTMLPage("/h2o/CoefficientChart.html","chart"));
    registerRequest(new Cancel());
    registerRequest(new DRFModelView());
    registerRequest(new DRFProgressPage());
    registerRequest(new DownloadDataset());
    registerRequest(new Exec2());
    registerRequest(new ExportS3Progress());
    registerRequest(new GBMModelView());
    registerRequest(new GBMProgressPage());
    registerRequest(new GLMGridProgress());
    registerRequest(new GLMProgressPage());
    registerRequest(new GridSearchProgress());
    registerRequest(new LogView.LogDownload());
    registerRequest(new NeuralNetModelView());
    registerRequest(new NeuralNetProgressPage());
    registerRequest(new DeepLearningModelView());
    registerRequest(new DeepLearningProgressPage());
    registerRequest(new KMeans2Progress());
    registerRequest(new KMeans2ModelView());
    registerRequest(new PCAProgressPage());
    registerRequest(new PCAModelView());
    registerRequest(new PostFile());
    registerRequest(new water.api.Upload2.PostFile());
    registerRequest(new Progress());
    registerRequest(new Progress2());
    registerRequest(new PutValue());
    registerRequest(new RFTreeView());
    registerRequest(new RFView());
    registerRequest(new RReaderProgress());
    registerRequest(new Remove());
    registerRequest(new RemoveAll());
    registerRequest(new RemoveAck());
    registerRequest(new RunScript());
    registerRequest(new SetColumnNames());
    registerRequest(new water.api.SetColumnNames2());     // Set colnames for FluidVec objects
    registerRequest(new LogAndEcho());
    registerRequest(new ToEnum());
    registerRequest(new ToEnum2());
    registerRequest(new ToInt2());
    registerRequest(new GLMProgress());
    registerRequest(new hex.glm.GLMGridProgress());
    registerRequest(new water.api.Levels2());    // Temporary hack to get factor levels efficiently
    registerRequest(new water.api.Levels());    // Ditto the above for ValueArray objects
    // Typeahead
    registerRequest(new TypeaheadModelKeyRequest());
    registerRequest(new TypeaheadGLMModelKeyRequest());
    registerRequest(new TypeaheadRFModelKeyRequest());
    registerRequest(new TypeaheadKMeansModelKeyRequest());
    registerRequest(new TypeaheadPCAModelKeyRequest());
    registerRequest(new TypeaheadHexKeyRequest());
    registerRequest(new TypeaheadFileRequest());
    registerRequest(new TypeaheadHdfsPathRequest());
    registerRequest(new TypeaheadKeysRequest("Existing H2O Key", "", null));
    registerRequest(new TypeaheadS3BucketRequest());
    // testing hooks
    registerRequest(new TestPoll());
    registerRequest(new TestRedirect());
//    registerRequest(new GLMProgressPage2());
    registerRequest(new GLMModelView());
    registerRequest(new GLMGridView());
//    registerRequest(new GLMValidationView());
    registerRequest(new LaunchJar());
    Request.initializeNavBar();

    //v2 API
    registerRequest(new v2Parse());
    registerRequest(new v2PostFile());
    registerRequest(new v2ListUri());
    registerRequest(new v2RespPrev());
    registerRequest(new water.api.v2.Progress());
    registerRequest(new CancelJob());
  }

  /**
   * Registers the request with the request server.
   */
  public static Request registerRequest(Request req) {
    assert req.supportedVersions().length > 0;
    for (API_VERSION ver : req.supportedVersions()) {
      String href = req.href(ver);
      assert (! _requests.containsKey(href)) : "Request with href "+href+" already registered";
      _requests.put(href,req);
      req.registered(ver);
    }
    return req;
  }

  public static void unregisterRequest(Request req) {
    for (API_VERSION ver : req.supportedVersions()) {
      String href = req.href(ver);
      _requests.remove(href);
    }
  }

  // Keep spinning until we get to launch the NanoHTTPD
  public static void start() {
    new Thread( new Runnable() {
        @Override public void run()  {
          while( true ) {
            try {
              // Try to get the NanoHTTP daemon started
              SERVER = new RequestServer(H2O._apiSocket);
              break;
            } catch( Exception ioe ) {
              Log.err(Sys.HTTPD,"Launching NanoHTTP server got ",ioe);
              try { Thread.sleep(1000); } catch( InterruptedException e ) { } // prevent denial-of-service
            }
          }
        }
      }, "Request Server launcher").start();
  }

  public static String maybeTransformRequest (String uri) {
    if (uri.isEmpty() || uri.equals("/")) {
      return "/Tutorials.html";
    }

    Pattern p = Pattern.compile("/R/bin/([^/]+)/contrib/([^/]+)(.*)");
    Matcher m = p.matcher(uri);
    boolean b = m.matches();
    if (b) {
      // On Jenkins, this command sticks his own R version's number
      // into the package that gets built.
      //
      //     R CMD INSTALL -l $(TMP_BUILD_DIR) --build h2o-package
      //
      String versionOfRThatJenkinsUsed = "3.0";

      String platform = m.group(1);
      String version = m.group(2);
      String therest = m.group(3);
      String s = "/R/bin/" + platform + "/contrib/" + versionOfRThatJenkinsUsed + therest;
      return s;
    }

    return uri;
  }

  // uri serve -----------------------------------------------------------------
  void maybeLogRequest (String uri, String method, Properties parms) {
    boolean filterOutRepetitiveStuff = true;

    if (filterOutRepetitiveStuff) {
      if (uri.endsWith(".css")) return;
      if (uri.endsWith(".js")) return;
      if (uri.endsWith(".png")) return;
      if (uri.endsWith(".ico")) return;
      if (uri.startsWith("/Typeahead")) return;
      if (uri.startsWith("/2/Typeahead")) return;
      if (uri.startsWith("/Cloud.json")) return;
      if (uri.endsWith("LogAndEcho.json")) return;
      if (uri.contains("Progress")) return;
      if (uri.startsWith("/Jobs.json")) return;
    }

    String log = String.format("%-4s %s", method, uri);
    for( Object arg : parms.keySet() ) {
      String value = parms.getProperty((String) arg);
      if( value != null && value.length() != 0 )
        log += " " + arg + "=" + value;
    }
    Log.info(Sys.HTTPD, log);
  }

  @Override public NanoHTTPD.Response serve( String uri, String method, Properties header, Properties parms ) {
    // Jack priority for user-visible requests
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    // update arguments and determine control variables
    uri = maybeTransformRequest(uri);
    // determine the request type
    Request.RequestType type = Request.RequestType.requestType(uri);
    String requestName = type.requestName(uri);

    maybeLogRequest(uri, method, parms);
    try {
      // determine if we have known resource
      Request request = _requests.get(requestName);
      // if the request is not know, treat as resource request, or 404 if not
      // found
      if (request == null)
        return getResource(uri);
      // Some requests create an instance per call
      request = request.create(parms);
      // call the request
      return request.serve(this,parms,type);
    } catch( Exception e ) {
      if(!(e instanceof ExpectedExceptionForDebug))
        e.printStackTrace();
      // make sure that no Exception is ever thrown out from the request
      parms.setProperty(Request.ERROR,e.getClass().getSimpleName()+": "+e.getMessage());
      return _http500.serve(this,parms,type);
    }
  }

  private RequestServer( ServerSocket socket ) throws IOException {
    super(socket,null);
  }

  // Resource loading ----------------------------------------------------------

  // Returns the response containing the given uri with the appropriate mime
  // type.
  private NanoHTTPD.Response getResource(String uri) {
    byte[] bytes = _cache.get(uri);
    if( bytes == null ) {
      InputStream resource = Boot._init.getResource2(uri);
      if (resource != null) {
        try {
          bytes = ByteStreams.toByteArray(resource);
        } catch( IOException e ) { Log.err(e); }
        byte[] res = _cache.putIfAbsent(uri,bytes);
        if( res != null ) bytes = res; // Racey update; take what is in the _cache
      }
      Closeables.closeQuietly(resource);
    }
    if ((bytes == null) || (bytes.length == 0)) {
      // make sure that no Exception is ever thrown out from the request
      Properties parms = new Properties();
      parms.setProperty(Request.ERROR,uri);
      return _http404.serve(this,parms,Request.RequestType.www);
    }
    String mime = NanoHTTPD.MIME_DEFAULT_BINARY;
    if (uri.endsWith(".css"))
      mime = "text/css";
    else if (uri.endsWith(".html"))
      mime = "text/html";
    // return new NanoHTTPD.Response(NanoHTTPD.HTTP_OK,mime,new ByteArrayInputStream(bytes));
    NanoHTTPD.Response res = new NanoHTTPD.Response(NanoHTTPD.HTTP_OK,mime,new ByteArrayInputStream(bytes));
    res.addHeader("Content-Length", Long.toString(bytes.length));
    // res.addHeader("Content-Disposition", "attachment; filename=" + uri);
    return res;
  }

}
