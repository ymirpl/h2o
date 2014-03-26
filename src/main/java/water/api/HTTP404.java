package water.api;

import java.util.Properties;

import water.NanoHTTPD;
import water.util.RString;

/**
 *
 * @author peta
 */
public class HTTP404 extends Request {

  private transient final Str _error = new Str(ERROR,"Unknown error");

  public HTTP404() {
    _requestHelp = "Displays the HTTP 404 page with error specified in JSON"
            + " argument error.";
    _error._requestHelp = "Error description for the 404. Generally the URL not found.";
  }

  @Override public Response serve() {
    return Response.error(_error.value());
  }

  @Override protected String serveJava() {
    return _error.value();
  }

  @Override public water.NanoHTTPD.Response serve(NanoHTTPD server, Properties parms, RequestType type) {
    water.NanoHTTPD.Response r = super.serve(server, parms, type);
    r.status = NanoHTTPD.HTTP_NOTFOUND;
    return r;
  }

  private static final String _html =
            "<h3>HTTP 404 - Not Found</h3>"
          + "<div class='alert alert-error'>%ERROR</div>"
          ;

  @Override protected String build(Response response) {
    StringBuilder sb = new StringBuilder();
    sb.append("<div class='container'>");
    sb.append("<div class='row-fluid'>");
    sb.append("<div class='span12'>");
    sb.append(buildResponseHeader(response));
    RString str = new RString(_html);
    str.replace("ERROR", response.error());
    sb.append(str.toString());
    sb.append("</div></div></div>");
    return sb.toString();
  }

}
