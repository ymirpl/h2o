package water.parser;

import java.util.ArrayList;

import water.Iced;

public final class ValueString extends Iced implements Comparable<ValueString> {
   private byte [] _buf;
   private int _off;
   private int _len;

   public ValueString() {}

   public ValueString(byte [] buf, int off, int len){
     _buf = buf;
     _off = off;
     _len = len;
   }

   public ValueString(String from) {
     _buf = from.getBytes();
     _off = 0;
     _len = get_buf().length;
   }

   public ValueString(byte [] buf){
     this(buf,0,buf.length);
   }

   @Override public int compareTo( ValueString o ) {
     int len = Math.min(_len,o._len);
     for( int i=0; i<len; i++ ) {
       int x = (0xFF&_buf[_off+i]) - (0xFF&o._buf[o._off+i]);
       if( x != 0 ) return x;
     }
     return _len - o._len;
   }

   @Override
   public int hashCode(){
     int hash = 0;
     int n = get_off() + get_length();
     for (int i = get_off(); i < n; ++i)
       hash = 31 * hash + get_buf()[i];
     return hash;
   }

   void addChar(){_len++;}

   void addBuff(byte [] bits){
     byte [] buf = new byte[get_length()];
     int l1 = get_buf().length-get_off();
     System.arraycopy(get_buf(), get_off(), buf, 0, l1);
     System.arraycopy(bits, 0, buf, l1, get_length()-l1);
     _off = 0;
     _buf = buf;
   }


// WARNING: LOSSY CONVERSION!!!
  // Converting to a String will truncate all bytes with high-order bits set,
  // even if they are otherwise a valid member of the field/ValueString.
  // Converting back to a ValueString will then make something with fewer
  // characters than what you started with, and will fail all equals() tests.a
  @Override public String toString(){
    return new String(_buf,_off,_len);
  }

  public static String[] toString( ValueString vs[] ) {
    if( vs==null ) return null;
    String[] ss = new String[vs.length];
    for( int i=0; i<vs.length; i++ )
      ss[i] = vs[i].toString();
    return ss;
  }

  void set(byte [] buf, int off, int len){
    _buf = buf;
    _off = off;
    _len = len;
  }

  public ValueString setTo(String what) {
    _buf = what.getBytes();
    _off = 0;
    _len = _buf.length;
    return this;
  }

  @Override public boolean equals(Object o){
    if(!(o instanceof ValueString)) return false;
    ValueString str = (ValueString)o;
    if(str.get_length() != get_length())return false;
    for(int i = 0; i < get_length(); ++i)
      if(get_buf()[get_off()+i] != str.get_buf()[str.get_off()+i]) return false;
    return true;
  }
  public final byte [] get_buf() {return _buf;}
  public final int get_off() {return _off;}
  public final int get_length() {return _len;}
}
