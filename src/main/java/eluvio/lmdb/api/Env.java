/*
 * Copyright 2015 Eluvio (http://www.eluvio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eluvio.lmdb.api;

import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;

public class Env implements AutoCloseable {
  private static enum State { INIT, OPEN, CLOSED }
  
  protected final Pointer env;
  private volatile State state = State.INIT;
  
  public Env() {
    PointerByReference ref = new PointerByReference();
    ApiErrors.checkError("mdb_env_create", Api.instance.mdb_env_create(ref));
    env = ref.getValue();
  }
  
  public int maxKeySize() {
    return Api.instance.mdb_env_get_maxkeysize(env);
  }
  
  public synchronized void setMapSize(long size) {
    if (State.CLOSED == state) throw new RuntimeException("Env is closed");
    
    if (State.OPEN == state) {
      // Need to make sure there are no outstanding transactions
      EnvInfo info = info();
      if (info.numReaders > 0) throw new RuntimeException("Can't call mdb_env_set_mapsize while there are open transactions");
    }
    
    ApiErrors.checkError("mdb_env_set_mapsize", Api.instance.mdb_env_set_mapsize(env, size));
  }
  
  public void setMaxReaders(int readers) {
    if (State.INIT != state) throw new RuntimeException("Can only call setMaxReaders if the environment has not been opened");
    ApiErrors.checkError("mdb_env_set_maxreaders", Api.instance.mdb_env_set_maxreaders(env, readers));
  }
  
  public void setMaxDBs(int dbs) {
    if (State.INIT != state) throw new RuntimeException("Can only call setMaxDBs if the environment has not been opened");
    ApiErrors.checkError("mdb_env_set_maxdbs", Api.instance.mdb_env_set_maxdbs(env, dbs));
  }
  
  /**
   * Enable the MDB_NOMETASYNC flag
   */
  public void disableMetaSync() {
    setFlags(Api.MDB_NOMETASYNC, true);
  }
  
  /**
   * Disable the MDB_NOMETASYNC flag
   * 
   * Flush system buffers to disk only once per transaction, omit the metadata flush. Defer 
   * that until the system flushes files to disk, or next non-MDB_RDONLY commit or mdb_env_sync(). 
   * This optimization maintains database integrity, but a system crash may undo the last 
   * committed transaction. I.e. it preserves the ACI (atomicity, consistency, isolation) but 
   * not D (durability) database property.
   */
  public void enableMetaSync() {
    setFlags(Api.MDB_NOMETASYNC, false);
  }
  
  /**
   * Enable the MDB_NOSYNC flag
   * 
   * Don't flush system buffers to disk when committing a transaction. This optimization means a
   * system crash can corrupt the database or lose the last transactions if buffers are not yet 
   * flushed to disk. The risk is governed by how often the system flushes dirty buffers to disk 
   * and how often mdb_env_sync() is called. However, if the filesystem preserves write order and 
   * the MDB_WRITEMAP flag is not used, transactions exhibit ACI (atomicity, consistency, 
   * isolation) properties and only lose D (durability). I.e. database integrity is maintained, 
   * but a system crash may undo the final transactions.
   */
  public void disableSync() {
    setFlags(Api.MDB_NOSYNC, true);
  }
  
  /**
   * Disable the MDB_NOSYNC flag
   */
  public void enableSync() {
    setFlags(Api.MDB_NOSYNC, false);
  }
  
  private void setFlags(int flags, boolean enableOrDisable) {
    ApiErrors.checkError("mdb_env_set_flags", Api.instance.mdb_env_set_flags(env, flags, enableOrDisable ? 1 : 0));
  }

  public void open(String path) {
    open(path, 0);
  }
  
  public void open(String path, int flags) {
    open(path, flags, 0644);
  }
  
  public synchronized void open(String path, int flags, int mode) {
    if (State.INIT != state) throw new RuntimeException("Env is either already open or is closed");
    
    final int rc = Api.instance.mdb_env_open(env, path, flags, mode);
    
    // If this function fails, mdb_env_close() must be called to discard the MDB_env handle.
    if (0 != rc) close();
    
    // This will throw an exception if the rc is not zero
    ApiErrors.checkError("mdb_env_open", rc);
    
    state = State.OPEN;
  }
  
  public synchronized void close() {
    if (State.CLOSED == state) return;
    Api.instance.mdb_env_close(env);
    state = State.CLOSED;
  }
  
  public EnvInfo info() {
    Api.MDB_envinfo info = new Api.MDB_envinfo();;
    ApiErrors.checkError("mdb_env_info", Api.instance.mdb_env_info(env, info));
    return new EnvInfo(info);
  }
  
  public Stat stat() {
    Api.MDB_stat stat = new Api.MDB_stat();
    ApiErrors.checkError("mdb_env_info", Api.instance.mdb_env_stat(env, stat));
    return new Stat(stat);
  }
  
  public Txn beginTxn() {
    return new Txn(this);
  }
  
  public Txn beginTxn(int flags) {
    return new Txn(this, flags);
  }
  
  public Txn beginTxn(Txn parent, int flags) {
    return new Txn(this, parent, flags);
  }
  
  public void sync() {
    sync(false);
  }
  
  public void sync(boolean force) {
    ApiErrors.checkError("mdb_env_sync", Api.instance.mdb_env_sync(env, force ? 1 : 0));
  }
}
