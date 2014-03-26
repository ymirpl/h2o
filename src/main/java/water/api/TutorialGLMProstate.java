/**
 *
 */
package water.api;


/**
 * Basic page introducing tutorial for GLM on prostate dataset.
 *
 * @author michal
 *
 */
public class TutorialGLMProstate extends TutorialWorkflow {

  private final transient TutorWorkflow _wf;
  private final static String[][] TUTORIAL_STEPS = new String[][]{
    /*               Title     Short Summary     File containing step description */
    new String[] { "Step 1", "Introduction"  ,     "/tutorials/glm.prostate/step1.html" },
    new String[] { "Step 2", "Dataset inhale",     "/tutorials/glm.prostate/step2.html" },
    new String[] { "Step 3", "Parsing dataset",    "/tutorials/glm.prostate/step3.html" },
    new String[] { "Step 4", "Inspecting dataset", "/tutorials/glm.prostate/step4.html" },
    new String[] { "Step 5", "Building model",     "/tutorials/glm.prostate/step5.html" },
    new String[] { "Step 6", "Inspecting model",   "/tutorials/glm.prostate/step6.html" },
    new String[] { "Step 7", "Using model",        "/tutorials/glm.prostate/step7.html" } };

  public TutorialGLMProstate() {
    _wf = new TutorWorkflow("GLM Tutorial");
    int i = 1;
    for (String[] info : TUTORIAL_STEPS) {
      _wf.addStep(i++, new FileTutorStep(info));
    }
  }

  @Override
  protected TutorWorkflow getWorkflow() {
    return _wf;
  }
}
