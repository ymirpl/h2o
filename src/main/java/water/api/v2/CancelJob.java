package water.api.v2;

import water.Job;
import water.Key;
import water.api.Request;
import water.api.RequestArguments.Str;
import water.api.RequestServer.API_VERSION;

public class CancelJob extends Request {
  final Str _job  = new Str(JOB);

  @Override protected String href(API_VERSION v) {
    return v.prefix() + "cancel_job";
  }

  @Override protected Response serve() {
    Job job = findJob();
    job.cancel();

    return Response.EMPTY_RESPONSE;
  }

  protected Job findJob() {
    Key key = Key.make(_job.value());
    return Job.findJob(key);
  }

}
