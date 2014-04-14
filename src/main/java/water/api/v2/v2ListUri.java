package water.api.v2;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import water.api.Request;
import water.api.RequestServer.API_VERSION;

public class v2ListUri extends Request {
  private final Str _uri = new Str("uri");

  @Override protected Response serve() {
    JsonObject json = new JsonObject();
    json.add("uris", new JsonPrimitive(_uri.value()));
    Response r = Response.done(json);

    return r;
  }

  @Override protected String href(API_VERSION v) {
    return v.prefix() + "list_uri";
  }
}
