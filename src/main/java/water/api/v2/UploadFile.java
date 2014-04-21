
package water.api.v2;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import water.Key;
import water.api.*;
import water.api.RequestServer.API_VERSION;

  public class UploadFile extends JSONOnlyRequest {
    H2OKey key = new H2OKey(FILENAME,true);

    @Override public String href() { return href(API_VERSION.V_v2); }
    @Override protected String href(API_VERSION v) {
      return v.prefix() + "post_file";
    }

    @Override protected Response serve() {
      JsonObject response = new JsonObject();
      response.addProperty("dst", key.value().toString());
      //response.add("uri", new JsonPrimitive(key.toString()));
      //response.add("size", new JsonPrimitive(0));
      //response.add("dst", new JsonPrimitive(Key.make().toString()));
      return Response.custom(response);
    }
  }
