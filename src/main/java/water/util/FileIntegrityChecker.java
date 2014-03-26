package water.util;

import java.io.File;
import java.util.ArrayList;

import water.*;
import water.fvec.*;
import water.persist.PersistNFS;

public class FileIntegrityChecker extends DRemoteTask<FileIntegrityChecker> {
  final String   _root;         // Root of directory
  final String[] _files;        // File names found locally
  final long  [] _sizes;        // File sizes found locally
  final boolean  _newApi;       // Produce NFSFileVec instead of ValueArray
  int[] _ok;                    // OUTPUT: files which are globally compatible


  @Override public void lcompute() {
    _ok = new int[_files.length];
    for (int i = 0; i < _files.length; ++i) {
      File f = new File(_files[i]);
      if (f.exists() && (f.length()==_sizes[i]))
        _ok[i] = 1;
    }
    tryComplete();
  }

  @Override public void reduce(FileIntegrityChecker o) {
    if( _ok == null ) _ok = o._ok;
    else Utils.add(_ok,o._ok);
  }

  @Override public byte priority() { return H2O.GUI_PRIORITY; }

  private void addFolder(File folder, ArrayList<File> filesInProgress ) {
    if( !folder.canRead() ) return;
    if (folder.isDirectory()) {
      for (File f: folder.listFiles()) {
        if( !f.canRead() ) continue; // Ignore unreadable files
        if( f.isHidden() && !folder.isHidden() )
          continue;             // Do not dive into hidden dirs unless asked
        if (f.isDirectory())
          addFolder(f,filesInProgress);
        else
          filesInProgress.add(f);
      }
    } else {
      filesInProgress.add(folder);
    }
  }

  public static FileIntegrityChecker check(File r, boolean newApi) {
    return new FileIntegrityChecker(r,newApi).invokeOnAllNodes();
  }

  public FileIntegrityChecker(File root, boolean newApi) {
    _root = PersistNFS.decodeFile(new File(root.getAbsolutePath())).toString();
    _newApi = newApi;
    ArrayList<File> filesInProgress = new ArrayList();
    addFolder(root,filesInProgress);
    _files = new String[filesInProgress.size()];
    _sizes = new long[filesInProgress.size()];
    for (int i = 0; i < _files.length; ++i) {
      File f = filesInProgress.get(i);
      _files[i] = f.getAbsolutePath();
      _sizes[i] = f.length();
    }
  }

  public int size() { return _files.length; }
  public String getFileName(int i) { return _files[i]; }

  // Sync this directory with H2O.  Record all files that appear to be visible
  // to the entire cloud, and give their Keys.  List also all files which appear
  // on this H2O instance but are not consistent around the cluster, and Keys
  // which match the directory name but are not on disk.
  public Key syncDirectory(ArrayList<String> files,
                           ArrayList<String> keys,
                           ArrayList<String> fails,
                           ArrayList<String> dels) {

    // Remove & report all Keys that match the root prefix
    for( Key k : H2O.localKeySet() )
      if( k.toString().startsWith(_root) ) {
        dels.add(k.toString());
        Lockable.delete(k);
      }

    Futures fs = new Futures();
    Key k = null;
    // Find all Keys which match ...
    for( int i = 0; i < _files.length; ++i ) {
      if( _ok[i] < H2O.CLOUD.size() ) {
        if( fails != null ) fails.add(_files[i]);
      } else {
        File f = new File(_files[i]);
        k = PersistNFS.decodeFile(f);
        if( files != null ) files.add(_files[i]);
        if( keys  != null ) keys .add(k.toString());
        if(_newApi) {
          NFSFileVec nfs = DKV.get(NFSFileVec.make(f, fs)).get();
          new Frame(k,new String[] { "0" }, new Vec[] { nfs }).delete_and_lock(null).unlock(null);
        } else {
          long size = f.length();
          Value val;
          if(size < 2*ValueArray.CHUNK_SZ){
            val = new Value(k,(int)size,Value.NFS);
            val.setdsk();
          }else
            val = new Value(k,new ValueArray(k,size),Value.NFS);
          DKV.put(k, val, fs);
        }
      }
    }
    fs.blockForPending();
    return k;
  }
}
