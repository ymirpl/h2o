
package water.api;

import java.util.Arrays;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.parser.*;
import water.parser.CustomParser.PSetupGuess;
import water.util.Utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class StoreView extends Request {

  protected Str _filter = new Str(FILTER, "");
  protected final Int _offset = new Int(OFFSET,0,0,1024);
  protected final Int _view = new Int(VIEW, 20, 0, 1024);

  @Override protected Response serve() {
    JsonObject result = new JsonObject();
    // get the offset index
    int offset = _offset.value();
    // write the response
    H2O cloud = H2O.CLOUD; // Current eldest Cloud
    Key[] keys = new Key[_view._max]; // Limit size of what we'll display on this page
    int len = 0;
    String filter = _filter.value();
    // Gather some keys that pass all filters
    for( Key key : H2O.globalKeySet(null) ) {
      if( filter != null && // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue; // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null ) continue; // Ignore misses
      keys[len++] = key; // Capture the key
      if( len == keys.length ) {
        // List is full; stop
        result.addProperty(Constants.MORE,true);
        break;
      }
    }
    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    if ((offset>=len) && (offset != 0))
      return Response.error("Only "+len+" keys available");

    // Now build the result JSON with all available keys
    JsonArray ary = new JsonArray();
    for( int i=offset; i<offset+_view.value(); i++ ) {
      if( i >= len ) break;
      Value val = DKV.get(keys[i]);
      if( val != null ) {
        JsonObject jo = formatKeyRow(cloud,keys[i],val);
        ary.add(jo);
      }
    }

    result.add(KEYS,ary);
    result.addProperty(NUM_KEYS, len);
    result.addProperty(CLOUD_NAME, H2O.NAME);
    result.addProperty(NODE_NAME, H2O.SELF.toString());
    Response r = Response.done(result);
    r.addHeader(
        "<form class='well form-inline' action='StoreView.html'>" +
        " <input type='text' class='input-small span10' placeholder='filter' " +
        "    name='filter' id='filter' value='"+_filter.value()+"' maxlength='512'>" +
        " <button type='submit' class='btn btn-primary'>Filter keys!</button>" +
        "</form>");

    r.setBuilder(KEYS, new PaginatedTable(argumentsToJson(),offset,_view.value(), len, false));
    r.setBuilder(KEYS+"."+KEY, new KeyCellBuilder());
    r.setBuilder(KEYS+".col_0", new KeyMinAvgMaxBuilder());
    r.setBuilder(KEYS+".col_1", new KeyMinAvgMaxBuilder());
    r.setBuilder(KEYS+".col_2", new KeyMinAvgMaxBuilder());
    r.setBuilder(KEYS+".col_3", new KeyMinAvgMaxBuilder());
    r.setBuilder(KEYS+".col_4", new KeyMinAvgMaxBuilder());
    r.setBuilder(MORE, new HideBuilder());
    return r;
  }

  static private String noNaN( double d ) {
    return (Double.isNaN(d) || Double.isInfinite(d)) ? "" : Double.toString(d);
  }

  private JsonObject formatKeyRow(H2O cloud, Key key, Value val) {
    JsonObject result = new JsonObject();
    result.addProperty(KEY, key.toString());
    result.addProperty(VALUE_SIZE,val.length());

    JsonObject mt = new JsonObject();
    JsonObject jcols[] = new JsonObject[]{mt,mt,mt,mt,mt};
    long rows = -1;
    int cols = -1;
    String str = "";

    // See if this is a structured ValueArray. Report results from a total parse.
    if( val.isArray() ) {
      ValueArray ary = val.get();
      if( ary._cols.length > 1 || ary._cols[0]._size != 1 ) {
        rows = ary._numrows;
        cols = ary._cols.length;
        result.addProperty(ROWS,rows); // exact rows
        result.addProperty(COLS,cols); // exact cols
        for( int i = 0; i < jcols.length; ++i ) {
          JsonObject col = new JsonObject();
          if (i < cols) {
            ValueArray.Column c = ary._cols[i];
            if (c._size!=0) {
              col.addProperty(HEADER,c._name);
              if( c._domain==null ) {
                col.addProperty(MIN , noNaN(c._min ));
                col.addProperty(MEAN, noNaN(c._mean));
                col.addProperty(MAX , noNaN(c._max ));
              } else if( c._domain.length > 0 ) {
                int max = c._domain.length;
                col.addProperty(MIN , c._domain[0]);
                col.addProperty(MEAN, c._domain[max/2]);
                col.addProperty(MAX , c._domain[max-1]);
              }
            }
          }
          jcols[i] = col;
        }
      }
    }
    if(val.isFrame()){
      Frame fr = val.get();
      rows = fr.numRows();
      cols = fr.numCols();
      result.addProperty(ROWS,rows); // exact rows
      result.addProperty(COLS,cols); // exact cols
      for( int i = 0; i < jcols.length; ++i ) {
        JsonObject col = new JsonObject();
        if (i < cols) {
          Vec v = fr.vecs()[i];
          col.addProperty(HEADER,fr._names[i]);
          if( !v.isEnum()) {
            col.addProperty(MIN , noNaN(v.min() ));
            col.addProperty(MEAN, noNaN(v.mean()));
            col.addProperty(MAX , noNaN(v.max() ));
          } else if( v.domain().length > 0 ) {
            int max = v.domain().length;
            col.addProperty(MIN , v.domain()[0]);
            col.addProperty(MEAN, v.domain()[max/2]);
            col.addProperty(MAX , v.domain()[max-1]);
          }
        }
        jcols[i] = col;
      }
    }

/*
   // Whatever this is trying to do, it's not helping.
   // I think this is trying to decode data files that have been POSTed, but instead it's just
   // corrupting StoreView output.
   // Maybe turn this on again once it understands how to disambiguate different data types.
   //
   // Tom

    if( rows == -1 ) {
      byte [] bits = Utils.getFirstUnzipedBytes(val);
      PSetupGuess sguess = ParseDataset.guessSetup(bits);
      if(sguess != null &&  sguess.valid() && sguess._data != null && sguess._data.length >= 4 && sguess._setup._ncols > 0 ) { // Able to parse sanely?
        int zipped_len = val.getFirstBytes().length;
        double bytes_per_row = (double) zipped_len / sguess._data.length;
        rows = (long) (val.length() / bytes_per_row);
        cols = sguess._setup._ncols;
        result.addProperty(ROWS, "~" + rows);
        result.addProperty(COLS, cols);
        final int len = sguess._data.length;
        for( int i=0; i<Math.min(cols,jcols.length); i++ ) {
          JsonObject col = new JsonObject();
          if(len > 0 && i < sguess._data[0].length) col.addProperty(HEADER,sguess._data[0][i]); // First 4 rows, including labels
          if(len > 1 && i < sguess._data[1].length) col.addProperty(MIN   ,sguess._data[1][i]); // as MIN/MEAN/MAX
          if(len > 2 && i < sguess._data[2].length) col.addProperty(MEAN  ,sguess._data[2][i]);
          if(len > 3 && i < sguess._data[3].length) col.addProperty(MAX   ,sguess._data[3][i]);
          jcols[i] = col;
        }
      } else {
        result.addProperty(ROWS,""); // no rows
        result.addProperty(COLS,"");
      }
      // Now the first 100 bytes of Value as a String
      StringBuilder sb = new StringBuilder();
      byte[] b = bits;   // Unzipped bits, if any
      int newlines=0;
      int len = Math.min(b.length,100);
      for( int i=0; i<len; i++ ) {
        byte c = b[i];
        if( c == '&' ) sb.append("&amp;");
        else if( c == '<' ) sb.append("&lt;");
        else if( c == '>' ) sb.append("&gt;");
        else if( c == '\r' ) ;    // ignore windows crlf
        else if( c == '\n' ) {    // newline
          if( ++newlines >= 4 ) break; // limit to 4 lines visually
          sb.append("<br>");           // visual newline
        } else if( c == ',' && i+1<len && b[i+1]!=' ' )
          sb.append(", ");
        else if( c < 32 ) sb.append('?');
        else sb.append((char)c);
      }
      if( val.length() > len ) sb.append("...");
      str = sb.toString();
    }
*/
    // Instead of the brokenness above, just paste in the empty string.
    result.addProperty(ROWS,"");
    result.addProperty(COLS,"");

    for( int i=0; i<jcols.length; i++ )
      result.add("col_"+i,jcols[i]);

    result.addProperty(VALUE,str); // VALUE last in the JSON
    return result;
  }
}
