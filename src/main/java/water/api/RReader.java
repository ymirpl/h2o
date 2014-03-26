package water.api;

import hex.rf.RFModel;

import java.io.IOException;

import water.*;
import water.H2O.H2OCountedCompleter;
import water.util.Log;
import water.util.RString;

import com.google.gson.JsonObject;

public class RReader extends Request {
  protected final H2OExistingKey _source = new H2OExistingKey(SOURCE_KEY);
  protected final H2OKey         _dest   = new H2OKey(DEST_KEY, false);

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='RReader.html?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override
  protected Response serve() {
    Value source = _source.value();
    Key dest = _dest.value();
    if( dest == null ) {
      String n = source._key.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 )
        n = n.substring(0, dot);
      dest = Key.make(RFModel.KEY_PREFIX + n);
    }
    try {
      final Value source_ = source;
      final Key dest_ = dest;
      H2O.submitTask(new H2OCountedCompleter() {
          @Override public void compute2() {
            try {
              water.RReader.run(dest_, source_.openStream());
            } catch( IOException e ) {
              Log.err(e);
            }
          }
        });
      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.DEST_KEY, dest.toString());

      Response r = RReaderProgress.redirect(response, dest);
      r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
      return r;
    } catch( Throwable e ) {
      return Response.error(e);
    }
  }
}
