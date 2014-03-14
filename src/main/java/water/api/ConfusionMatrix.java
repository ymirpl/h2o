package water.api;

import water.MRTask2;
import water.Model;
import water.Request2;
import water.UKV;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.TransfVec;
import water.fvec.Vec;
import water.util.Utils;

import java.util.Arrays;

import static water.util.Utils.printConfusionMatrix;

/**
 *  Compare two categorical columns, reporting a grid of co-occurrences.
 *
 *  <p>The semantics follows R-approach - see R code:
 *  <pre>
 *  > l = c("A", "B", "C")
 *  > a = factor(c("A", "B", "C"), levels=l)
 *  > b = factor(c("A", "B", "A"), levels=l)
 *  > confusionMatrix(a,b)
 *
 *            Reference
 * Prediction A B C
 *          A 1 0 0
 *          B 0 1 0
 *          C 1 0 0
 *  </pre></p>
 *
 *  <p>Note: By default we report zero rows and columns.</p>
 *
 *  @author cliffc
 */
public class ConfusionMatrix extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "", required = true, filter = Default.class)
  public Frame actual;

  @API(help="Column of the actual results (will display vertically)", required=true, filter=actualVecSelect.class)
  public Vec vactual;
  class actualVecSelect extends VecClassSelect { actualVecSelect() { super("actual"); } }

  @API(help = "", required = true, filter = Default.class)
  public Frame predict;

  @API(help="Column of the predicted results (will display horizontally)", required=true, filter=predictVecSelect.class)
  public Vec vpredict;
  class predictVecSelect extends VecClassSelect { predictVecSelect() { super("predict"); } }

  @API(help="domain of the actual response")
  String [] actual_domain;
  @API(help="domain of the predicted response")
  String [] predicted_domain;
  @API(help="union of domains")
  public
  String [] domain;
  @API(help="Confusion Matrix (or co-occurrence matrix)")
  public long cm[][];

  @API(help="Mean Squared Error")
  public double mse = Double.NaN;

  private boolean classification;

  @Override public Response serve() {
    Vec va = null,vp = null, avp = null;
    classification = vactual.isInt() && vpredict.isInt();
    // Input handling
    if( vactual==null || vpredict==null )
      throw new IllegalArgumentException("Missing actual or predict!");
    if (vactual.length() != vpredict.length())
      throw new IllegalArgumentException("Both arguments must have the same length!");

    try {
      if (classification) {
        // Create a new vectors - it is cheap since vector are only adaptation vectors
        va = vactual .toEnum(); // always returns TransfVec
        actual_domain = va._domain;
        vp = vpredict.toEnum(); // always returns TransfVec
        predicted_domain = vp._domain;
        if (!Arrays.equals(actual_domain, predicted_domain)) {
          domain = Utils.union(actual_domain, predicted_domain);
          int[][] vamap = Model.getDomainMapping(domain, actual_domain, true);
          va = TransfVec.compose( (TransfVec) va, vamap, domain, false ); // delete original va
          int[][] vpmap = Model.getDomainMapping(domain, predicted_domain, true);
          vp = TransfVec.compose( (TransfVec) vp, vpmap, domain, false ); // delete original vp
        } else domain = actual_domain;
        // The vectors are from different groups => align them, but properly delete it after computation
        if (!va.group().equals(vp.group())) {
          avp = vp;
          vp = va.align(vp);
        }
        cm = new CM(domain.length).doAll(va,vp)._cm;
      } else {
        if (vactual.isEnum())
          throw new IllegalArgumentException("Actual vector cannot be categorical for regression scoring.");
        if (vpredict.isEnum())
          throw new IllegalArgumentException("Predicted vector cannot be categorical for regression scoring.");
        mse = new CM(1).doAll(vactual,vpredict).mse();
      }
      return Response.done(this);
    } catch( Throwable t ) {
      return Response.error(t);
    } finally {       // Delete adaptation vectors
      if (va!=null) UKV.remove(va._key);
      if (vp!=null) UKV.remove(vp._key);
      if (avp!=null) UKV.remove(avp._key);
    }
  }

  // Compute the co-occurrence matrix
  private static class CM extends MRTask2<CM> {
    /* @IN */ final int _c_len;
    /* @OUT Classification */ long _cm[][];
    /* @OUT Regression */ public double mse() { return _count > 0 ? _mse/_count : Double.POSITIVE_INFINITY; }
    /* @OUT Regression Helper */ private double _mse;
    /* @OUT Regression Helper */ private long _count;
    CM(int c_len) { _c_len = c_len;  }
    @Override public void map( Chunk ca, Chunk cp ) {
      //classification
      if (_c_len > 1) {
        _cm = new long[_c_len+1][_c_len+1];
        int len = Math.min(ca._len,cp._len); // handle different lenghts, but the vectors should have been rejected already
        for( int i=0; i < len; i++ ) {
          int a=ca.isNA0(i) ? _c_len : (int)ca.at80(i);
          int p=cp.isNA0(i) ? _c_len : (int)cp.at80(i);
          _cm[a][p]++;
        }
        if( len < ca._len )
          for( int i=len; i < ca._len; i++ )
            _cm[ca.isNA0(i) ? _c_len : (int)ca.at80(i)][_c_len]++;
        if( len < cp._len )
          for( int i=len; i < cp._len; i++ )
            _cm[_c_len][cp.isNA0(i) ? _c_len : (int)cp.at80(i)]++;
      } else {
        _cm = null;
        _mse = 0;
        assert(ca._len == cp._len);
        int len = ca._len;
        for( int i=0; i < len; i++ ) {
          if (ca.isNA0(i) || cp.isNA0(i)) continue; //TODO: Improve
          final double a=ca.at0(i);
          final double p=cp.at0(i);
          _mse += (p-a)*(p-a);
          _count++;
        }
      }
    }

    @Override public void reduce( CM cm ) {
      if (_cm != null && cm._cm != null) {
        Utils.add(_cm,cm._cm);
      } else {
        assert(_mse != Double.NaN && cm._mse != Double.NaN);
        assert(_cm == null && cm._cm == null);
        _mse += cm._mse;
        _count += cm._count;
      }
    }
  }

  @Override public boolean toHTML( StringBuilder sb ) {
    if (classification) {
      DocGen.HTML.section(sb,"Confusion Matrix");
      if( cm == null ) return true;
      printConfusionMatrix(sb, cm, domain, true);
    } else{
      DocGen.HTML.section(sb,"Mean Squared Error");
      if( mse == Double.NaN ) return true;
      DocGen.HTML.arrayHead(sb);
      sb.append("<tr class='warning'><td>" + mse + "</td></tr>");
      DocGen.HTML.arrayTail(sb);
    }
    return true;
  }

  public void toASCII( StringBuilder sb ) {
    if (classification) {
      if(cm == null) return;
      printConfusionMatrix(sb, cm, domain, false);
    } else {
      sb.append("MSE: " + mse);
    }
  }
}
