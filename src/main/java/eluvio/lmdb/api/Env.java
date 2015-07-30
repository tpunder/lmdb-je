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
