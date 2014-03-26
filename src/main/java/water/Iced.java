package water;

/**
 * Empty marker class.  Used by the auto-serializer.
 */
public abstract class Iced implements Freezable, Cloneable {
  // The abstract methods to be filled in by subclasses.  These are automatically
  // filled in by any subclass of Iced during class-load-time, unless one
  // is already defined.  These methods are NOT DECLARED ABSTRACT, because javac
  // thinks they will be called by subclasses relying on the auto-gen.
  private RuntimeException barf() {
    return new RuntimeException(getClass().toString()+" should be automatically overridden in the subclass by the auto-serialization code");
  }
  @Override public AutoBuffer write(AutoBuffer bb) { return bb; }
  @Override public <T extends Freezable> T read(AutoBuffer bb) { return (T)this; }
  @Override public <T extends Freezable> T newInstance() { throw barf(); }
  @Override public int frozenType() { throw barf(); }
  @Override public AutoBuffer writeJSONFields(AutoBuffer bb) { return bb; }
  public AutoBuffer writeJSON(AutoBuffer bb) { return writeJSONFields(bb.put1('{')).put1('}'); }
  @Override public water.api.DocGen.FieldDoc[] toDocField() { return null; }

  public Iced init( Key k ) { return this; }
  @Override public Iced clone() {
    try { return (Iced)super.clone(); }
    catch( CloneNotSupportedException e ) { throw water.util.Log.errRTExcept(e); }
  }
}
