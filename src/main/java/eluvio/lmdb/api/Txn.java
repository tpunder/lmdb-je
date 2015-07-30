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

import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;

/**
 * Represents a LMDB transaction created via mdb_txn_begin()
 * <p>
 * <b>NOTE:</b>
 * <ul>
 *   <li>Transactions must be closed (via abort(), commit(), or close())</li>
 *   <li>Transactions can only be used by the thread that created them</li>
 * </ul>
 */
public class Txn implements AutoCloseable {
  private static enum State { INIT, OPEN, CLOSED }
  
  protected final Pointer txn;
  protected final Env env;
  public final Txn parent;
  private final Thread thread;
  public final boolean readOnly;
  protected volatile State state;
  protected volatile List<Runnable> onAbortOrCommit = null;
  
  /**
   * We want to make sure cursors get closed with when the transaction is
   * closed so we track them here.  For the common case there will only be a
   * single cursor per transaction so we just assign it to this variable.
   * <p>
   * If there is more than one cursor using this transaction then we switch
   * to using the {@link #cursors} hash map to track them.
   */
  protected volatile Cursor currentCursor = null;
  
  /**
   * If there is more than one cursor using this transaction then we switch
   * to using this hash map to keep track of them and stop using
   * {@link #currentCursor}
   */
  protected volatile ConcurrentHashMap<Cursor,Boolean> cursors = null;
  
  /**
   * Create a new transaction in the given environment.  This is the same as calling {@link Env#beginTxn()}
   * @param env The Env to create the transaction in
   * @see       Env#beginTxn()
   */
  public Txn(Env env) {
    this(env, null, 0);
  }
  
  /**
   * Create a new transaction in the given environment with the given flags
   * @param env   The Env to create the transaction in
   * @param flags Flags to pass to the underlying mdb_txn_begin()
   * @see         Env#beginTxn()
   */
  public Txn(Env env, int flags) {
    this(env, null, flags);
  }
  
  /**
   * 
   * @param env    The Env to create the transaction in
   * @param parent The parent read-write transaction (if this is a nested transaction) otherwise null
   * @see          Env#beginTxn()
   */
  public Txn(Env env, Txn parent) {
    this(env, parent, 0);
  }
  
  /**
   * 
   * @param env    The Env to create the transaction in
   * @param parent The parent read-write transaction (if this is a nested transaction) otherwise null
   * @param flags  Flags to pass to the underlying mdb_txn_begin() call
   * @see          Env#beginTxn()
   */
  public Txn(Env env, Txn parent, int flags) {
    this.env = env;
    this.parent = parent;
    this.thread = Thread.currentThread();
    this.readOnly = (Api.MDB_RDONLY & flags) == Api.MDB_RDONLY;
    
    if (null != parent) parent.threadCheck();
    
    PointerByReference ref = new PointerByReference();
    int rc = Api.instance.mdb_txn_begin(env.env, null == parent ? null : parent.txn, flags, ref);
    
    if (ApiErrors.MDB_MAP_RESIZED == rc) {
      // Need to pickup the new size and retry the mdb_txn_begin call.
      env.setMapSize(0);
      rc = Api.instance.mdb_txn_begin(env.env, null == parent ? null : parent.txn, flags, ref);
    }
    
    ApiErrors.checkError("mdb_txn_begin", rc);
    
    this.txn = ref.getValue();
    state = State.OPEN;
  }
  
  /**
   * mdb_txn_commit
   */
  public void commit() {
    threadCheck();
    if (State.OPEN != state) throw new RuntimeException("Cannot commit Txn since it is not OPEN");
    closeCursors();
    ApiErrors.checkError("mdb_txn_commit", Api.instance.mdb_txn_commit(txn));
    state = State.CLOSED;
    runAbortOrCommitCallbacks();
  }
  
  /**
   * mdb_txn_abort
   */
  public void abort() {
    // Ignoring the threadCheck() for now since LMDBEnv.closeTransactions() can call this method
    // from a thread that is different from the thread that created the transaction.  This is probably
    // unsafe!!!!!!!
    //threadCheck();
    if (State.CLOSED == state) throw new RuntimeException("Cannot abort Txn since it is already CLOSED");
    closeCursors();
    Api.instance.mdb_txn_abort(txn);
    state = State.CLOSED;
    runAbortOrCommitCallbacks();
  }
  
  /**
   * mdb_txn_reset
   */
  public void reset() {
    threadCheck();
    if (State.OPEN != state) throw new RuntimeException("Cannot reset Txn since it is not OPEN");
    if (!readOnly) throw new RuntimeException("mdb_txn_reset can only be called for a read-only transaction");
    closeCursors();
    Api.instance.mdb_txn_reset(txn);
    state = State.INIT;
  }
  
  /**
   * mdb_txn_renew
   */
  public void renew() {
    threadCheck();
    if (State.INIT != state) throw new RuntimeException("Cannot renew Txn since it is either already OPEN or CLOSED");
    if (!readOnly) throw new RuntimeException("mdb_txn_renew can only be called for a read-only transaction");
    Api.instance.mdb_txn_renew(txn);
    state = State.OPEN;
  }
  
  /**
   * Throws an exception if the current thread is not the same thread that created the transaction.
   */
  protected void threadCheck() {
    if (Thread.currentThread() != thread) throw new RuntimeException("Transaction can only be used by the thread that created it.  Creating Thread: "+thread+"  Current Thread: "+Thread.currentThread());
  }
  
  /**
   * Implements the AutoCloseable interface.  The behavior depends on the
   * current state of the transaction.  If it's open then commit()
   * will be called.  If it's been reset() but not renewed then abort()
   * is called.  If the transaction is already closed then nothing happens. 
   */
  public void close() {
    switch(state) {
      case INIT:
        // Abort the reset transaction
        abort();
        break;
      case OPEN:
        // Commit the open transaction
        commit();
        break;
      case CLOSED:
        // Do nothing
        break;
    }
  }
  
  /**
   * Is the transaction currently open?
   * @return true if the transaction is open
   */
  public boolean isOpen() {
    return State.OPEN == state;
  }
  
  /**
   * Is the transaction currently closed (i.e. has abort() or commit() been called)?
   * @return true if the transaction is closed
   */
  public boolean isClosed() {
    return State.CLOSED == state;
  }
  
  /**
   * Is the transaction in the INIT state? (i.e. reset() has been called but renew() has not yet been called)
   * @return true if the transaction is in the INIT state
   */
  public boolean isInit() {
    return State.INIT == state;
  }
  
  void registerCursor(Cursor c) {
    assert null == cursors || null == currentCursor;
    
    // We should be using either the currentCursor or the cursors variable
    if (null != cursors) {
      cursors.put(c, Boolean.TRUE);
      return;
    }
    
    if (null == currentCursor) {
      currentCursor = c;
      return;
    }
    
    cursors = new ConcurrentHashMap<Cursor,Boolean>();
    cursors.put(currentCursor, Boolean.TRUE);
    cursors.put(c, Boolean.TRUE);
    currentCursor = null;
  }
  
  void deregisterCursor(Cursor c) {
    assert null == cursors || null == currentCursor;
    
    if (c == currentCursor) {
      currentCursor = null;
    } else if (null != cursors && cursors.remove(c, Boolean.TRUE)) {
      // good
    } else {
      throw new IllegalStateException("Missing cursor");
    }
  }
  
  private void closeCursors() {
    assert null == cursors || null == currentCursor;
    
    if (null != currentCursor) {
      currentCursor.close();
      currentCursor = null;
    }
    
    if (null != cursors) {
      for (Cursor c : cursors.keySet()) {
        c.close();
      }
      
      cursors = null;
    }
  }
  
  public void onAbortOrCommit(Runnable callback) {
    if (null == onAbortOrCommit) onAbortOrCommit = new LinkedList<Runnable>();
    onAbortOrCommit.add(callback);
  }
  
  private void runAbortOrCommitCallbacks() {
    if (null == onAbortOrCommit) return;
    
    for (Runnable callback : onAbortOrCommit) {
      callback.run();
    }
    
    onAbortOrCommit = null;
  }
}
