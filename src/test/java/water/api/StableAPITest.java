/**
 *
 */
package water.api;

import hex.pca.PCAModel;
import hex.*;
import hex.gbm.GBM;
import hex.glm.*;
import hex.pca.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;

import org.junit.*;

import water.*;
import water.api.LogView.LogDownload;
import water.api.Request.API;
import water.api.RequestArguments.Argument;
import water.api.Upload.PostFile;

/**
 * The objective of test is to test a stability of API.
 *
 * It is filled by REST api calls and their arguments used by Python/R code.
 * The test tests if the arguments are published by REST API in Java code.
 *
 * Note: this is a pure JUnit test, no cloud is launched
 */
public class StableAPITest {

  /** Mapping between REST API methods and their attributes used by Python code. */
  static  Map< Class<? extends Request>, String[]> pyAPI = new HashMap<Class<? extends Request>, String[]>();
  /** Mapping between REST API methods and their attributes used by R code. */
  static  Map< Class<? extends Request>, String[]> rAPI  = new HashMap<Class<? extends Request>, String[]>();

  /** Test compatibility of defined Python calls with REST API published by Java code. */
  @Test
  public void testPyAPICompatibility() {
    testAPICompatibility("Python API", pyAPI);
  }

  /** Test compatibility of defined R calls with REST API published by Java code. */
  @Test
  public void testRAPICompatibility() {
    testAPICompatibility("R API", rAPI);
  }

  /** Test given client APIs calls against REST API published by Java code. */
  private void testAPICompatibility(String client, Map<Class<? extends Request>, String[]> api ) {
    Map<Class<? extends Request>, String[]> unsupportedParams = new HashMap<Class<? extends Request>, String[]>();
    for (Map.Entry<Class<? extends Request>, String[]> apiCall : api.entrySet())  {
      String[] unsParams = verifyAPICall(client, apiCall.getKey(), apiCall.getValue());
      if (unsParams!=null && unsParams.length > 0) unsupportedParams.put(apiCall.getKey(), unsParams);
    }
    // Do not fail here now
    Assert.assertTrue(f(client, unsupportedParams), unsupportedParams.isEmpty());
  }

  /**
   * Verify given <code>api</code> client's call containing given parameters <code>params</code>.
   */
  private <T extends Request> String[] verifyAPICall(String client, Class<T> api, String[] params) {
    List<String> unsupportedParams = new ArrayList<String>(5);
    List<Field> apiParams = getAllParams(api, Request.class, Request2.class.isAssignableFrom(api) ? Request2FFilter : Request1FFilter) ;
    T request_v1 = Request2.class.isAssignableFrom(api) ? null : newInstance(api);
    //if (Request2.class.isAssignableFrom(api)) System.err.println(apiParams);
    for (String par : params) {
        // Handle Request2 API - parameters directly corresponds to REST attributes
        if (Request2.class.isAssignableFrom(api)) {
          if (!contains(par, apiParams)) unsupportedParams.add(par);
        } else if (Request.class.isAssignableFrom(Request.class)) {
          // Handle original Request
          //   - in this case we need to look into Argument itself and search for name exposed as REST attribute
          assert request_v1 != null;
          if (!supportsArg(request_v1, par, apiParams)) unsupportedParams.add(par);
        }
    }
    return unsupportedParams.isEmpty() ? null : unsupportedParams.toArray(new String[unsupportedParams.size()]);
  }

  static <T extends Request> T newInstance(Class<T> api) {
    Assert.assertTrue("The test should instantiat only Request API not Request2 API", !Request2.class.isAssignableFrom(api));
    try {
      return api.newInstance();
    } catch( Exception e ) {
      e.printStackTrace();
      Assert.assertTrue("Test should be able to instantiate " + api + " via default ctor!", false);
    }
    return null;
  }

  static abstract class FFilter { abstract boolean involve(Field f); }
  static FFilter Request1FFilter = new FFilter() { @Override boolean involve(Field f) { return Argument.class.isAssignableFrom(f.getType()); } };
  static FFilter Request2FFilter = new FFilter() { @Override boolean involve(Field f) { return contains(API.class, f.getDeclaredAnnotations()); } };
  static boolean contains(Class<? extends Annotation> annoType, Annotation[] annotations) {
    for (Annotation anno : annotations) if (anno.annotationType().equals(annoType)) return true;
    return false;
  }
  static boolean contains(String s, List<Field> params) {
    return find(s,params)!=null;
  }
  static Field find(String name, List<Field> params) {
    for (Field f : params) if (name.equals(f.getName())) return f;
    return null;
  }
  static <T extends Request> boolean supportsArg(T api, String name, List<Field> params) {
    // Go through all the fields and take their values and search for JSON arg name
    for (Field f : params) {
      assert Argument.class.isAssignableFrom(f.getType());
      try {
        Argument arg = (Argument) f.get(api);
        if (name.equals(arg._name)) return true;
      } catch( Exception e ) {
      }
    }
    // No matching Java API argument found
    return false;
  }
  private static List<Field> getAllParams(Class<?> startClass, Class<?> parentClass, FFilter ffilter) {
    List<Field> params = new ArrayList<Field>(10);
    Class<?> cls = startClass;
    while (cls!=null && cls!=parentClass) {
      Field[] fields = cls.getDeclaredFields();
      for (Field f : fields) if (ffilter==null || ffilter.involve(f)) { f.setAccessible(true); params.add(f); }
      cls = cls.getSuperclass();
    }
    return params;
  }

  // Initialize all required static fields, BUT DO NOT START THE CLOUD
  @BeforeClass
  static public void initTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    H2O.NAME = "Test cloud";
    // Add a new item into TYPE_MAP
    Field fMap = TypeMap.class.getDeclaredField("MAP");
    fMap.setAccessible(true);
    Map<String,Integer> map = (Map<String, Integer>) fMap.get(null);
    map.put(PCAModel.class.getName(), 1000);
  }

  /**
   * Register all existing Python API calls and used parameters.
   *
   * All of them were collected from Jenkins commands.log.
   */
  @BeforeClass
  static public void registerPyAPI() {
    regPy(Cancel.class, "key");
    regPy(Cloud.class);
    regPy(ConfusionMatrix.class, "actual", "predict", "vactual", "vpredict");
    regPy(DRFModelView.class);
    regPy(DRFProgressPage.class);
    regPy(Debug.class);
    regPy(DownloadDataset.class, "src_key");
    regPy(ExportHdfs.class);
    regPy(ExportS3.class);
    regPy(ExportS3Progress.class);
    regPy(GBM.class, "cols", "destination_key", "learn_rate", "max_depth", "min_rows", "nbins", "ntrees", "response", "source", "validation");
    regPy(GBMModelView.class, "_modelKey");
    regPy(GBMProgressPage.class);
    regPy(GLM.class, "alpha", "beta_epsilon", "case", "case_mode", "destination_key", "family", "key", "lambda", "link", "lsm_solver", "max_iter", "n_folds", "standardize", "thresholds", "tweedie_power", "weight", "x", "y");
    regPy(GLMGrid.class, "alpha", "beta_eps", "case", "case_mode", "family", "key", "lambda", "link", "max_iter", "n_folds", "parallel", "thresholds", "weight", "x", "y");
    regPy(GLMGridProgress.class, "destination_key", "job");
    regPy(GLMProgressPage.class, "destination_key", "job", "progress_key");
    regPy(GLMScore.class, "key", "model_key", "thresholds");
    regPy(GeneratePredictionsPage.class, "data_key", "destination_key", "model_key");
    regPy(Get.class, "key");
    regPy(HTTP404.class);
    regPy(HTTP500.class);
    regPy(IOStatus.class);
    regPy(ImportFiles.class, "path");
    regPy(ImportFiles2.class, "path");
    regPy(ImportHdfs.class, "path");
    regPy(ImportS3.class, "bucket");
    regPy(ImportUrl.class);
    regPy(Inspect.class, "key", "max_column_display", "offset", "view");
    regPy(Inspect2.class, "offset", "src_key");
    regPy(JStack.class);
    regPy(Jobs.class);
    regPy(KMeans.class, "cols", "destination_key", "initialization", "k", "max_iter", "normalize", "seed", "source_key");
    regPy(KMeansApply.class, "data_key", "destination_key", "model_key");
    regPy(KMeansScore.class, "key", "model_key");
    regPy(LogView.class);
    regPy(NeuralNet.class, "activation", "cols", "destination_key", "epochs", "hidden", "l2", "rate", "response", "source");
    // regPy(PCA.class, "destination_key", "key", "standardize", "tolerance");
    regPy(PCA.class, "destination_key", "source", "standardize", "tolerance");
    // regPy(PCAScore.class, "destination_key", "key", "model_key", "num_pc");
    regPy(PCAScore.class, "source", "model", "destination_key", "num_pc");
    regPy(Parse.class, "destination_key", "header", "header_from_file", "separator", "source_key");
    //regPy(Parse.class, "alpha", "bin_limit", "case", "case_mode", "class_weights", "depth", "destination_key", "exclude", "family", "features", "header", "header_from_file", "ignore", "iterative_cm", "key_key", "lambda", "link", "model_key", "n_folds", "ntree", "out_of_bag_error_estimate", "parallel", "response_variable", "sample", "sampling_strategy", "seed", "separator", "source_key", "stat_type", "strata_samples", "use_non_local_data", "x", "y");
    regPy(Parse2.class, "destination_key", "header", "source_key");
    regPy(PostFile.class, "key"); // PostFile has no key attribute - it is hard-coded in Nano
    regPy(Predict.class, "data", "model", "prediction");
    regPy(Progress.class, "destination_key", "job");
    regPy(Progress2.class);
    regPy(PutValue.class, "key", "value");
    regPy(QuantilesPage.class);
    regPy(RF.class, "bin_limit", "class_weights", "data_key", "depth", "exclusive_split_limit", "features", "ignore", "iterative_cm", "model_key", "ntree", "out_of_bag_error_estimate", "parallel", "response_variable", "sample", "sampling_strategy", "seed", "stat_type", "strata_samples", "use_non_local_data");
    regPy(RFScore.class);
    regPy(RFTreeView.class, "data_key", "model_key", "tree_number");
    regPy(RFView.class, "class_weights", "clear_confusion_matrix", "data_key", "iterative_cm", "model_key", "ntree", "out_of_bag_error_estimate", "response_variable");
    regPy(RReader.class);
    regPy(RReaderProgress.class);
    regPy(Remove.class, "key");
    regPy(RemoveAck.class);
    regPy(Score.class);
    regPy(Script.class);
    regPy(SetColumnNames.class);
    regPy(Shutdown.class);
    regPy(StoreView.class, "filter", "offset", "view");
    regPy(SummaryPage.class, "key", "max_column_display");
    regPy(SummaryPage2.class);
    regPy(TestPoll.class, "hoho");
    regPy(TestRedirect.class);
    regPy(Timeline.class);
    regPy(Upload.class);
  }

  /**
   * Used R API extracted from R/h2o-package/R/Internal.R
   */
  @BeforeClass
  static public void registerRAPI() {
    regR(Cloud.class);
    regPy(DownloadDataset.class, "src_key");
    regR(GeneratePredictionsPage.class, "model_key", "data_key");
    regR(GBM.class, "destination_key", "source", "response", "cols", "ntrees", "max_depth", "learn_rate", "min_rows", "classification");
    regR(GBMModelView.class, "_modelKey");
    regR(GLM.class, "key", "y", "x", "family", "n_folds", "alpha", "lambda", "expert_settings", "beta_epsilon", "standardize", "case_mode", "case", "tweedie_power");
    regR(GLM2.class, "source", "destination_key", "response", "ignored_cols", "family", "n_folds", "alpha", "lambda", "standardize", "tweedie_variance_power");
    regR(GLMGrid.class, "key", "y", "x", "family", "n_folds", "alpha", "lambda", "case_mode", "case", "parallel" );
    regR(GLMGridProgress.class, "destination_key");
    regR(GLMModelView.class, "_modelKey");
//    regR(GLMValidationView.class, "_valKey");
    regR(ImportFiles.class, "path");
    regR(ImportHdfs.class, "path");
    regR(ImportUrl.class, "url", "key" );
    regR(Inspect.class, "key");
    regR(Inspect2.class, "src_key");
    regR(Jobs.class);
    regR(LogDownload.class);
    regR(KMeans.class, "source_key", "k", "max_iter", "cols");
    regR(KMeansApply.class, "model_key", "data_key", "destination_key");
    regR(KMeansScore.class, "model_key", "key");
    regR(Parse.class, "source_key", "destination_key", "header", "header_from_file", "separator" );
    // regR(PCA.class, "key", "x", "destination_key", "max_pc", "tolerance", "standardize");
    regR(PCA.class, "source", "ignored_cols", "destination_key", "max_pc", "tolerance", "standardize");
    // regR(PCAScore.class, "model_key", "key", "destination_key", "num_pc");
    regR(PCAScore.class, "source", "model", "destination_key", "num_pc");
    regR(Predict.class, "model", "data", "prediction");
    regR(Remove.class, "key");
    regR(RF.class, "data_key", "response_variable", "ignore", "ntree", "depth", "class_weights", "model_key");
    regR(RFView.class, "model_key", "data_key", "response_variable", "ntree", "class_weights", "out_of_bag_error_estimate");
    regR(RFTreeView.class, "model_key", "tree_number", "data_key");
    regR(SetColumnNames.class, "target", "copy_from" );
    regR(StoreView.class);
    regR(SummaryPage.class, "key");

  }

  // Register an API method used by Python
  static <T extends Request> void regPy(Class<T> api, String... params) { add(pyAPI, api, params); }
  // Register an API method used by R
  static <T extends Request> void regR(Class<T> api, String... params)  { add(rAPI, api, params); }
  static <T extends Request> void add(Map< Class<? extends Request>, String[]> apis, Class<T> api, String[] params) {
    if (apis.containsKey(api)) {
      String[] regParams = apis.get(api); // already registered params
      String[] pars = Arrays.copyOf(regParams, regParams.length + params.length);
      System.arraycopy(params, 0, pars, regParams.length, params.length);
      apis.put(api, pars);
    } else apis.put(api, params);
  }

  static String f(String client, Map<Class<? extends Request>, String[]> params) {
    StringBuilder sb = new StringBuilder(client).append(" uses the following unsupported parameters (arguments are not published by REST API)\n");
    for (Map.Entry<Class<? extends Request>, String[]> call : params.entrySet() )  {
      sb.append(call.getKey()).append(" : ").append(Arrays.toString(call.getValue())).append('\n');
    }
    return sb.toString();
  }

}
