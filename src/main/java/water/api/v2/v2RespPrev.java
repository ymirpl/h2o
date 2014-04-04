package water.api.v2;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

import water.*;
import water.api.DocGen.FieldDoc;
import water.api.*;
import water.api.Constants.Extensions;
import water.api.RequestArguments.*;
import water.api.RequestBuilders.Response;
import water.api.RequestServer.API_VERSION;
import water.api.v2.v2Parse.*;
import water.parser.*;

import com.google.gson.*;

public class v2RespPrev extends JSONOnlyRequest {

  private   final ParserType     _parserType= new ParserType(PARSER_TYPE);
  private   final Separator      _separator = new Separator(SEPARATOR);
  private   final Bool           _header    = new Bool(HEADER,false,"Use first line as a header");
  protected final Bool           _sQuotes   = new Bool("single_quotes",false,"Enable single quotes as field quotation character");
  protected final HeaderKey      _hdrFrom   = new HeaderKey("header_from_file",false);
  protected final Str            _excludeExpression    = new Str("exclude","");
  protected final ExistingCSVKey _source    = new ExistingCSVKey(URIS);//SOURCE_KEY
  protected final H2OKey         _key       = new H2OKey(URIS,true);//SOURCE_KEY
  protected final NewH2OHexKey   _dest      = new NewH2OHexKey(DEST_KEY);
  protected final Bool           _blocking  = new Bool("blocking",false,"Synchronously wait until parse completes");
  @SuppressWarnings("unused")
  private   final Preview        _preview   = new Preview(PREVIEW);



  @Override protected String href(API_VERSION v) {
    return v.prefix() + "parse_preview";
  }

  @Override protected Response serve() {
    JsonObject response = new JsonObject();
    JsonObject parserConfig = new JsonObject();
    //response.addProperty(RequestStatics.JOB, job.self().toString());
    //response.addProperty(RequestStatics.DEST_KEY,dest.toString());

    PSetup psetup = _source.value();
    String [] header = psetup._setup._setup._columnNames;
    JsonArray uris = new JsonArray();
    uris.add(new JsonPrimitive(_key.value().toString()));
    JsonElement parserType  = new JsonPrimitive(_parserType.value().toString());
    JsonElement headerSeparator  = new JsonPrimitive(DEFAULT_DELIMS[new Integer(_separator.value().toString())].substring(0, 1));
    JsonElement dataSeparator  = new JsonPrimitive(DEFAULT_DELIMS[new Integer(_separator.value().toString())].substring(0, 1));
    JsonElement skipHeader  = new JsonPrimitive(new Boolean(_header.value().toString()));
    JsonElement previewLen = new JsonPrimitive(psetup._setup._data.length);


    JsonArray jHRowArray = new JsonArray();
    JsonObject jHObject = new JsonObject();

    String [] colnames = _source.value()._setup._setup._columnNames;
    for (int i=0;i<colnames.length;i++){
      jHObject = new JsonObject();
      jHObject.add("header", new JsonPrimitive(colnames[i]));
      jHObject.add("type", new JsonPrimitive("ENUM"));
      jHRowArray.add(jHObject);
    }


    response.add("uris", uris);
    response.add("columns", jHRowArray);
    response.add("parser_type", parserType);
    response.add("header_separator", headerSeparator);
    response.add("data_separator", dataSeparator);
    response.add("skip_header", skipHeader);
    if(_hdrFrom != null && _hdrFrom.value() != null){
      JsonElement headerFile  = new JsonPrimitive(_hdrFrom.value().toString());
      response.add("header_file", headerFile);
    }else{
      response.add("header_file", uris);
    }



    ///

    String[][] data = null;
    data = psetup._setup._data;

    StringBuilder sb = new StringBuilder();
    JsonArray jRowArray = new JsonArray();
    JsonArray jArray = new JsonArray();

    if( data != null ) {
      int j = 0;
      if( psetup._setup._setup._header && header != null) { // Obvious header display, if asked for
        if(header == data[0]) ++j;
      }
      String s2 = "";
      for( int i=j; i<data.length; i++ ) {
        jArray = new JsonArray();
        s2 = "";
        for( String s : data[i] ){
          jArray.add(new JsonPrimitive(s));
          s2+=s;
        }
        jRowArray.add(jArray);
      }
    }




    ///
    parserConfig.add("parser_config", response);
    parserConfig.add("preview_len", previewLen);
    parserConfig.add("preview", jRowArray);

    //Response r = Progress.redirect(response, job.self(), dest);
    //r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
    return Response.custom(parserConfig);
    //return Response.done(new JsonObject());
  }

  private class ParserType extends InputSelect<CustomParser.ParserType> {
    public ParserType(String name) {
      super(name,false);
      setRefreshOnChange();
      _values = new String [CustomParser.ParserType.values().length-1];
      int i = 0;
      for(CustomParser.ParserType t:CustomParser.ParserType.values())
        if(t != CustomParser.ParserType.XLSX)
          _values[i++] = t.name();
    }
    private final String [] _values;
    @Override protected String   queryDescription() { return "File type"; }
    @Override protected String[] selectValues()     {
      return _values;
    }
    @Override protected String[] selectNames()      {
      return _values;
    }
    @Override protected CustomParser.ParserType defaultValue() {
      return CustomParser.ParserType.AUTO;
    }
    public void setValue(CustomParser.ParserType pt){record()._value = pt;}
    @Override protected String   selectedItemValue(){
      return value() != null ? value().toString() : defaultValue().toString(); }
    @Override protected CustomParser.ParserType parse(String input) throws IllegalArgumentException {
      return  CustomParser.ParserType.valueOf(input);
    }
  }

  private class Separator extends InputSelect<Byte> {
    public Separator(String name) {
      super(name,false);
      setRefreshOnChange();
    }
    @Override protected String   queryDescription() { return "Utilized separator"; }
    @Override protected String[] selectValues()     { return DEFAULT_IDX_DELIMS;   }
    @Override protected String[] selectNames()      { return DEFAULT_DELIMS; }
    @Override protected Byte     defaultValue()     {return CsvParser.AUTO_SEP;}
    public void setValue(Byte b){record()._value = b;}
    @Override protected String selectedItemValue(){ return value() != null ? value().toString() : defaultValue().toString(); }
    @Override protected Byte parse(String input) throws IllegalArgumentException {
      Byte result = Byte.valueOf(input);
      return result;
    }
  }

  public class HeaderKey extends H2OExistingKey {
    public HeaderKey(String name, boolean required) {
      super(name, required);
    }
    @Override protected String queryElement() {
      StringBuilder sb = new StringBuilder(super.queryElement() + "\n");
      try{
        String [] colnames = _source.value()._setup._setup._columnNames;
        if(colnames != null){
          sb.append("<table class='table table-striped table-bordered'>").append("<tr><th>Header:</th>");
          for( String s : colnames ) sb.append("<th>").append(s).append("</th>");
          sb.append("</tr></table>");
        }
      } catch( Exception e ) { }
      return sb.toString();
    }

  }

  public class ExistingCSVKey extends TypeaheadInputText<PSetup> {
    public ExistingCSVKey(String name) {
      super(TypeaheadKeysRequest.class, name, true);
//      addPrerequisite(_parserType);
//      addPrerequisite(_separator);
    }


    @Override protected PSetup parse(String input) throws IllegalArgumentException {
      System.out.println("parse");
      Pattern p = makePattern(input);
      Pattern exclude = null;
      if(_hdrFrom.specified())
        _header.setValue(true);
      if(_excludeExpression.specified())
        exclude = makePattern(_excludeExpression.value());
      ArrayList<Key> keys = new ArrayList();
      // boolean badkeys = false;
      for( Key key : H2O.globalKeySet(null) ) { // For all keys
        if( !key.user_allowed() ) continue;
        String ks = key.toString();
        if( !p.matcher(ks).matches() ) // Ignore non-matching keys
          continue;
        if(exclude != null && exclude.matcher(ks).matches())
          continue;
        Value v2 = DKV.get(key);  // Look at it
        if( !v2.isRawData() ) // filter common mistake such as *filename* with filename.hex already present
          continue;
        keys.add(key);        // Add to list
      }
      if(keys.size() == 0 )
        throw new IllegalArgumentException("I did not find any keys matching this pattern!");
      Collections.sort(keys);   // Sort all the keys, except the 1 header guy
      // now we assume the first key has the header
      Key hKey = null;
      if(_hdrFrom.specified()){
        hKey = _hdrFrom.value()._key;
        _header.setValue(true);
      }
      boolean checkHeader = !_header.specified();
      boolean hasHeader = _header.value();
      CustomParser.ParserSetup userSetup =  new CustomParser.ParserSetup(_parserType.value(),_separator.value(),hasHeader);
      CustomParser.PSetupGuess setup = null;
      try {
       setup = ParseDataset.guessSetup(keys, hKey, userSetup,checkHeader);
      }catch(ParseDataset.ParseSetupGuessException e){
        throw new IllegalArgumentException(e.getMessage());
      }
      if(setup._isValid){
        if(setup._hdrFromFile != null)
          _hdrFrom.setValue(DKV.get(setup._hdrFromFile));
        if(!_header.specified())
          _header.setValue(setup._setup._header);
        else
          setup._setup._header = _header.value();
        if(!_header.value())
          _hdrFrom.disable("Header is disabled.");
        PSetup res = new PSetup(keys,null,setup);
        _parserType.setValue(setup._setup._pType);
        _separator.setValue(setup._setup._separator);
        _hdrFrom._hideInQuery = _header._hideInQuery = _separator._hideInQuery = setup._setup._pType != CustomParser.ParserType.CSV;
        Set<String> dups = setup.checkDupColumnNames();
        if(!dups.isEmpty())
          throw new IllegalArgumentException("Column labels must be unique but these labels are repeated: "  + dups.toString());
        return res;
      } else
        throw new IllegalArgumentException("Invalid parser setup. " + setup.toString());
    }

    private final String keyRow(Key k){
      return "<tr><td>" + k + "</td></tr>\n";
    }

    @Override
    public String queryComment(){
      if(!specified())return "";
      PSetup p = value();
      StringBuilder sb = new StringBuilder();
      if(p._keys.size() <= 10){
        for(Key k:p._keys)
          sb.append(keyRow(k));
      } else {
        int n = p._keys.size();
        for(int i = 0; i < 5; ++i)
          sb.append(keyRow(p._keys.get(i)));
        sb.append("<tr><td>...</td></tr>\n");
        for(int i = 5; i > 0; --i)
          sb.append(keyRow(p._keys.get(n-i)));
      }
      return
          "<div class='alert'><b> Found " + p._keys.size() +  " files matching the expression.</b><br/>\n" +
          "<table>\n" +
           sb.toString() +
          "</table></div>";
    }

    private Pattern makePattern(String input) {
      // Reg-Ex pattern match all keys, like file-globbing.
      // File-globbing: '?' allows an optional single character, regex needs '.?'
      // File-globbing: '*' allows any characters, regex needs '*?'
      // File-globbing: '\' is normal character in windows, regex needs '\\'
      String patternStr = input.replace("?",".?").replace("*",".*?").replace("\\","\\\\").replace("(","\\(").replace(")","\\)");
      Pattern p = Pattern.compile(patternStr);
      return p;
    }

    @Override protected PSetup defaultValue() { return null; }
    @Override protected String queryDescription() { return "An existing H2O key (or regex of keys) of CSV text"; }
  }

  // A Query String, which defaults to the source Key with a '.hex' suffix
  protected class NewH2OHexKey extends Str {
    NewH2OHexKey(String name) {
      super(name,null/*not required flag*/);
      addPrerequisite(_source);
    }
    @Override protected String defaultValue() {
      PSetup setup = _source.value();
      if( setup == null ) return null;
      String n = setup._keys.get(0).toString();
      // blahblahblah/myName.ext ==> myName
      int sep = n.lastIndexOf(File.separatorChar);
      if( sep > 0 ) n = n.substring(sep+1);
      int dot = n.lastIndexOf('.');
      if( dot > 0 ) n = n.substring(0, dot);
      if( !Character.isJavaIdentifierStart(n.charAt(0)) ) n = "X"+n;
      char[] cs = n.toCharArray();
      for( int i=1; i<cs.length; i++ )
        if( !Character.isJavaIdentifierPart(cs[i]) )
          cs[i] = '_';
      n = new String(cs);
      int i = 0;
      String res = n + Extensions.HEX;
      Key k = Key.make(res);
      while(DKV.get(k) != null)
        k = Key.make(res = n + ++i + Extensions.HEX);
      return res;
    }
    @Override protected String queryDescription() { return "Destination hex key"; }
  }

  // A Query Bool, which includes a pretty HTML-ized version of the first few
  // parsed data rows.  If the value() is TRUE, we display as-if the first row
  // is a label/header column, and if FALSE not.
  public class Preview extends Argument {
      Preview(String name) {
      super(name,false);
//      addPrerequisite(_source);
//      addPrerequisite(_separator);
//      addPrerequisite(_parserType);
//      addPrerequisite(_header);
      setRefreshOnChange();
    }
    @Override protected String queryElement() {
      // first determine the value to put in the field
      // if no original value was supplied, use the provided one
      String[][] data = null;
      PSetup psetup = _source.value();
      if(psetup == null)
        return _source.specified()?"<div class='alert alert-error'><b>Found no valid setup!</b></div>":"";
      StringBuilder sb = new StringBuilder();
      if(psetup._failedKeys != null){
        sb.append("<div class='alert alert-error'>");
        sb.append("<div>\n<b>Found " + psetup._failedKeys.length + " files which are not compatible with the given setup:</b></div>");
        int n = psetup._failedKeys.length;
        if(n > 5){
          sb.append("<div>" + psetup._failedKeys[0] + "</div>\n");
          sb.append("<div>" + psetup._failedKeys[1] + "</div>\n");
          sb.append("<div>...</div>");
          sb.append("<div>" + psetup._failedKeys[n-2] + "</div>\n");
          sb.append("<div>" + psetup._failedKeys[n-1] + "</div>\n");
        } else for(int i = 0; i < n;++i)
          sb.append("<div>" + psetup._failedKeys[n-1] + "</div>\n");
        sb.append("</div>\n");
      }
      String [] err = psetup._setup._errors;
      boolean hasErrors = err != null && err.length > 0;
      boolean parsedOk = psetup._setup._isValid;
      String parseMsgType = hasErrors?parsedOk?"warning":"error":"success";
      sb.append("<div class='alert alert-" + parseMsgType + "'><b>" + psetup._setup.toString() + "</b>");
      if(hasErrors)
        for(String s:err)sb.append("<div>" + s + "</div>");
      sb.append("</div>");
      if(psetup._setup != null)
        data = psetup._setup._data;
      //ls
      String [] header = psetup._setup._setup._columnNames;

      if( data != null ) {
        sb.append("<table class='table table-striped table-bordered'>");
        int j = 0;
        if( psetup._setup._setup._header && header != null) { // Obvious header display, if asked for
          sb.append("<tr><th>Row#</th>");
          for( String s : header ) sb.append("<th>").append(s).append("</th>");
          sb.append("</tr>");
          if(header == data[0]) ++j;
        }
        for( int i=j; i<data.length; i++ ) { // The first few rows
          sb.append("<tr><td>Row ").append(i-j).append("</td>");
          for( String s : data[i] ) sb.append("<td>").append(s).append("</td>");
          sb.append("</tr>");
        }
        sb.append("</table>");
      }
      return sb.toString();
    }
    @Override protected Object parse(String input) throws IllegalArgumentException {return null;}
    @Override protected Object defaultValue() {return null;}

    @Override protected String queryDescription() {
      return "Preview of the parsed data";
    }
    @Override protected String jsRefresh(String callbackName) {
      return "";
    }
    @Override protected String jsValue() {
      return "";
    }
  }

  /** List of white space delimiters */
  static final String[] WHITE_DELIMS = { "NULL", "SOH (start of heading)", "STX (start of text)", "ETX (end of text)", "EOT (end of transmission)",
    "ENQ (enquiry)", "ACK (acknowledge)", "BEL '\\a' (bell)", "BS '\b' (backspace)", "HT  '\\t' (horizontal tab)", "LF  '\\n' (new line)", " VT '\\v' (vertical tab)",
    "FF '\\f' (form feed)", "CR '\\r' (carriage ret)", "SO  (shift out)", "SI  (shift in)", "DLE (data link escape)", "DC1 (device control 1) ", "DC2 (device control 2)",
    "DC3 (device control 3)", "DC4 (device control 4)", "NAK (negative ack.)", "SYN (synchronous idle)", "ETB (end of trans. blk)", "CAN (cancel)", "EM  (end of medium)",
    "SUB (substitute)", "ESC (escape)", "FS  (file separator)", "GS  (group separator)", "RS  (record separator)", "US  (unit separator)", "' ' SPACE" };
  /** List of all ASCII delimiters */
  static final String[] DEFAULT_DELIMS     = new String[127];
  static final String[] DEFAULT_IDX_DELIMS = new String[127];
  static {
    int i = 0;
    for (i = 0; i < WHITE_DELIMS.length; i++) DEFAULT_DELIMS[i] = String.format("%s: '%02d'", WHITE_DELIMS[i],i);
    for (;i < 126; i++) {
      String s = null; // Escape HTML entities manually or use StringEscapeUtils from Apache
      switch ((char)i) {
        case '&': s = "&amp;"; break;
        case '<': s = "&lt;";  break;
        case '>': s = "&gt;";  break;
        case '\"': s = "&quot;"; break;
        default : s = Character.toString((char)i);
      }
      DEFAULT_DELIMS[i] = String.format("%s: '%02d'", s, i);
    }
    for (i = 0; i < 126; i++) DEFAULT_IDX_DELIMS[i] = String.valueOf(i);
    DEFAULT_DELIMS[i]     = "AUTO";
    DEFAULT_IDX_DELIMS[i] = String.valueOf(CsvParser.AUTO_SEP);
  }

  public static String quote(String string) {
    if (string == null || string.length() == 0) {
        return "\"\"";
    }

    char         c = 0;
    int          i;
    int          len = string.length();
    StringBuilder sb = new StringBuilder(len + 4);
    String       t;

    sb.append('"');
    for (i = 0; i < len; i += 1) {
        c = string.charAt(i);
        switch (c) {
        case '\\':
        case '"':
            sb.append('\\');
            sb.append(c);
            break;
        case '/':
//                if (b == '<') {
                sb.append('\\');
//                }
            sb.append(c);
            break;
        case '\b':
            sb.append("\\b");
            break;
        case '\t':
            sb.append("\\t");
            break;
        case '\n':
            sb.append("\\n");
            break;
        case '\f':
            sb.append("\\f");
            break;
        case '\r':
           sb.append("\\r");
           break;
        default:
            if (c < ' ') {
                t = "000" + Integer.toHexString(c);
                sb.append("\\u" + t.substring(t.length() - 4));
            } else {
                sb.append(c);
            }
        }
    }
    sb.append('"');
    return sb.toString();
}
}
