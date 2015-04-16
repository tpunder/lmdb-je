package eluvio.lmdb.api;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.locks.ReentrantLock;

import eluvio.lmdb.api.Api.MDB_val;
import jnr.ffi.Pointer;
import jnr.ffi.byref.IntByReference;

public class DB {
  protected final int dbi;
  protected final Env env;
  protected final MDBComparator keyComparator;
  protected final MDBComparator dupComparator;
  protected final boolean dupsort;
  
  /** The txn that has a pending mdb_dbi_open call we are waiting to commit() or abort() */
  private static volatile Txn pendingOpeningTxn = null;
  
  private static ReentrantLock pendingOpeningLock = new ReentrantLock();
  
  private static Runnable onAbortOrCommitCallback = new Runnable() {
    public void run() {
      pendingOpeningTxn = null;
      pendingOpeningLock.unlock();
    }
  };
  
  protected static class MDBComparator implements Api.MDB_cmp_func {
    private final Comparator<ByteBuffer> comparator;
    
    public MDBComparator(Comparator<ByteBuffer> comparator) {
      this.comparator = comparator;
    }
    
    public int call(Pointer a, Pointer b) {
      return comparator.compare(new MDB_val(a).asByteBuffer(), new MDB_val(b).asByteBuffer());
    }
  }
  
  /**
   * mdb_dbi_open(txn, null, 0)
   * @param txn the transaction to use when opening the db
   */
  public DB(Txn txn) {
    this(txn, null, 0);
  }
  
  /**
   * mdb_dbi_open(txn, null, 0)
   * @param txn the transaction to use when opening the db
   * @param keyComparator the comparator to use for keys, or null to use the default
   */
  public DB(Txn txn, Comparator<ByteBuffer> keyComparator) {
    this(txn, null, 0, keyComparator);
  }
  
  /**
   * mdb_dbi_open(txn, null, flags)
   * @param txn the transaction to use when opening the db
   * @param flags flags for the underlying mdb_dbi_open call
   */
  public DB(Txn txn, int flags) {
    this(txn, null, flags);
  }
  
  /**
   * mdb_dbi_open(txn, null, flags)
   * @param txn the transaction to use when opening the db
   * @param flags flags for the underlying mdb_dbi_open call
   * @param keyComparator the comparator to use for keys, or null to use the default
   */
  public DB(Txn txn, int flags, Comparator<ByteBuffer> keyComparator) {
    this(txn, null, flags, keyComparator);
  }
  
  /**
   * mdb_dbi_open(txn, null, flags)
   * @param txn the transaction to use when opening the db
   * @param flags flags for the underlying mdb_dbi_open call
   * @param keyComparator the comparator to use for keys, or null to use the default
   * @param dupComparator the comparator to use for values in a MDB_DUPSORT database, or null to use the default
   */
  public DB(Txn txn, int flags, Comparator<ByteBuffer> keyComparator, Comparator<ByteBuffer> dupComparator) {
    this(txn, null, flags, keyComparator);
  }
  
  /**
   * mdb_dbi_open(txn, name, 0)
   * @param txn the transaction to use when opening the db
   * @param name the name of the database to open
   */
  public DB(Txn txn, String name) {
    this(txn, name, 0);
  }
  
  /**
   * mdb_dbi_open
   * @param txn the transaction to use when opening the db
   * @param name the name of the database to open
   * @param flags flags for the underlying mdb_dbi_open call
   */
  public DB(Txn txn, String name, int flags) {
    this(txn, name, flags, null);
  }
  
  /**
   * mdb_dbi_open
   * @param txn the transaction to use when opening the db
   * @param name the name of the database to open
   * @param keyComparator the comparator to use for keys, or null to use the default
   */
  public DB(Txn txn, String name, Comparator<ByteBuffer> keyComparator) {
    this(txn, name, 0, keyComparator);
  }
  
  /**
   * mdb_dbi_open
   * @param txn the transaction to use when opening the db
   * @param name the name of the database to open
   * @param flags flags for the underlying mdb_dbi_open call
   * @param keyComparator the comparator to use for keys, or null to use the default
   */
  public DB(Txn txn, String name, int flags, Comparator<ByteBuffer> keyComparator) {
    this(txn, name, flags, keyComparator, null);
  }
  
  /**
   * mdb_dbi_open
   * @param txn the transaction to use when opening the db
   * @param name the name of the database to open
   * @param flags flags for the underlying mdb_dbi_open call
   * @param keyComparator the comparator to use for keys, or null to use the default
   * @param dupComparator the comparator to use for values in a MDB_DUPSORT database, or null to use the default
   */
  public DB(Txn txn, String name, int flags, Comparator<ByteBuffer> keyComparator, Comparator<ByteBuffer> dupComparator) {
    dupsort = (Api.MDB_DUPSORT & flags) == Api.MDB_DUPSORT;
    
    if (!dupsort && null != dupComparator) throw new IllegalArgumentException("dupComparator cannot be set if not using MDB_DUPSORT");
    
    this.env = txn.env;
    final IntByReference ref = new IntByReference();
    
    // From the LMDB docs about mdb_dbi_open():
    //
    // This function must not be called from multiple concurrent
    // transactions in the same process. A transaction that uses
    // this function must finish (either commit or abort) before
    // any other transaction in the process may use this function.
    pendingOpeningLock.lock();
    
    if (null != pendingOpeningTxn) {
      // If we are re-entering this thread (because we are in the same thread) then make sure it's the same transaction
      if (pendingOpeningTxn != txn) throw new AssertionError("Re-entering DB constructor with different txn!  pendingOpeningTxn: "+pendingOpeningTxn+"  txn: "+txn);
    } else {
      // Only register the the callback if we aren't re-entering
      pendingOpeningTxn = txn;
      txn.onAbortOrCommit(onAbortOrCommitCallback);
    }
    
    ApiErrors.checkError("mdb_dbi_open", Api.instance.mdb_dbi_open(txn.txn, name, flags, ref));
    
    dbi = ref.getValue();
    
    if (null != keyComparator) {
      // Not sure how JNR-FFI works with passing in a function so we'll be sure and hang onto
      // a reference to make sure it doesn't get GC'd.
      this.keyComparator = new MDBComparator(keyComparator);
      ApiErrors.checkError("mdb_set_compare", Api.instance.mdb_set_compare(txn.txn, dbi, this.keyComparator));
    } else {
      this.keyComparator = null;
    }
    
    if (null != dupComparator) {
      // Not sure how JNR-FFI works with passing in a function so we'll be sure and hang onto
      // a reference to make sure it doesn't get GC'd.
      this.dupComparator = new MDBComparator(dupComparator);
      ApiErrors.checkError("mdb_set_dupsort", Api.instance.mdb_set_dupsort(txn.txn, dbi, this.dupComparator));
    } else {
      this.dupComparator = null;
    }
  }
  
  /**
   * mdb_dbi_flags
   * @param txn the transaction to use
   * @return the flags currently set for the db
   */
  public int flags(Txn txn) {
    txn.threadCheck();
    final IntByReference ref = new IntByReference();
    ApiErrors.checkError("mdb_dbi_flags", Api.instance.mdb_dbi_flags(txn.txn, dbi, ref));
    return ref.getValue();
  }
  
  /**
   * mdb_stat
   * @param txn the transaction to use
   * @return the stats returned from the mdb_stat call
   */
  public Stat stat(Txn txn) {
    txn.threadCheck();
    Api.MDB_stat stat = new Api.MDB_stat();
    ApiErrors.checkError("mdb_stat", Api.instance.mdb_stat(txn.txn, dbi, stat));
    return new Stat(stat);
  }

  /**
   * mdb_get
   * @param txn the transaction to use
   * @param key the key to get data for
   * @return the data for the key, or null if the key does not exist
   */
  public ByteBuffer get(Txn txn, ByteBuffer key) {
    txn.threadCheck();
    Api.MDB_val data = new Api.MDB_val();
    final int rc = Api.instance.mdb_get(txn.txn, dbi, new Api.MDB_val(key), data);
    
    if (ApiErrors.MDB_NOTFOUND == rc) return null;
    
    ApiErrors.checkError("mdb_get", rc);
    return data.asByteBuffer();
  }
  
  /**
   * mdb_put
   * @param txn the transaction to use
   * @param key the key to store data for
   * @param data the data to store for the key
   * @return true if the put was successful, false if the key/data already existed (if you are using the MDB_NODUPDATA or MDB_NOOVERWRITE flags).
   */
  public boolean put(Txn txn, ByteBuffer key, ByteBuffer data) {
    txn.threadCheck();
    return put(txn, key, data, 0);
  }
  
  /**
   * mdb_put with the MDB_APPEND flag
   * @param txn the transaction to use
   * @param key the key to store data for
   * @param data the data to store for the key
   * @return true if the put was successful, false if the key/data already existed (if you are using the MDB_NODUPDATA or MDB_NOOVERWRITE flags).
   */
  public boolean append(Txn txn, ByteBuffer key, ByteBuffer data) {
    txn.threadCheck();
    return put(txn, key, data, Api.MDB_APPEND);
  }
  
  /**
   * mdb_put
   * @param txn the transaction to use
   * @param key the key to store data for
   * @param data the data to store for the key
   * @param flags flags for the underlying mdb_put call
   * @return true if the put was successful, false if the key/data already existed (if you are using the MDB_NODUPDATA or MDB_NOOVERWRITE flags).
   */
  public boolean put(Txn txn, ByteBuffer key, ByteBuffer data, int flags) {
    txn.threadCheck();
    if (key.remaining() < 1) throw new IllegalArgumentException("Key must be at least 1 byte long");
    final int rc = Api.instance.mdb_put(txn.txn, dbi, new Api.MDB_val(key), new Api.MDB_val(data), flags);
    
    if (0 == rc) return true;
    if (ApiErrors.MDB_KEYEXIST == rc) return false;
    
    throw ApiErrors.toException("mdb_put", rc);
  }
  
  /**
   * mdb_del
   * @param txn the transaction to use
   * @param key the key to delete
   * @return true if they key was deleted
   */
  public boolean delete(Txn txn, ByteBuffer key) {
    return delete(txn, key, null);
  }
  
  /**
   * mdb_del
   * @param txn the transaction to use
   * @param key the key to delete
   * @param data the data to delete (when using MDB_DUPSORT)
   * @return true if the key or key/data pair were deleted
   */
  public boolean delete(Txn txn, ByteBuffer key, ByteBuffer data) {
    txn.threadCheck();
    if (key.remaining() < 1) throw new IllegalArgumentException("Key must be at least 1 byte long");
    if (null != data && !dupsort) throw new IllegalArgumentException("The data parameter can only be set when using MDB_DUPSORT");

    final int rc = Api.instance.mdb_del(txn.txn, dbi, new Api.MDB_val(key), null == data ? null : new Api.MDB_val(data));
    
    if (0 == rc) return true;
    else if (ApiErrors.MDB_NOTFOUND == rc) return false;
    else throw ApiErrors.toException("mdb_del", rc);
  }
  
  /**
   * Calls mdb_put with the MDB_RESERVE flag
   * @param txn the transaction to use
   * @param key the key to reserve data for
   * @param size the size of the data
   * @return The data ByteBuffer that can be written into
   */
  public ByteBuffer reserve(Txn txn, ByteBuffer key, int size) {
    return reserve(txn, key, size, 0);
  }
  
  /**
   * Calls mdb_put with the MDB_RESERVE flag
   * @param txn the transaction to use
   * @param key the key to reserve data for
   * @param size the size of the data
   * @param flags flags (in addition to MDB_RESERVE) for the underlying mdb_put call
   * @return The data ByteBuffer that can be written into
   */
  public ByteBuffer reserve(Txn txn, ByteBuffer key, int size, int flags) {
    txn.threadCheck();
    if (key.remaining() < 1) throw new IllegalArgumentException("Key must be at least 1 byte long");
    Api.MDB_val data = new Api.MDB_val(size);
    ApiErrors.checkError("mdb_put", Api.instance.mdb_put(txn.txn, dbi, new Api.MDB_val(key), data, flags | Api.MDB_RESERVE));
    return data.asByteBuffer();
  }
  
  /**
   * mdb_cmp
   * @param txn the transaction to use
   * @param a the first key
   * @param b the second key
   * @return 0 if the keys are equal, &lt; 0 if a is less than b and &gt; 0 if b is greater than a
   */
  public int compare(Txn txn, ByteBuffer a, ByteBuffer b) {
    txn.threadCheck();
    return Api.instance.mdb_cmp(txn.txn, dbi, new Api.MDB_val(a), new Api.MDB_val(b));
  }
  
  /**
   * mdb_dcmp
   * @param txn the transaction to use
   * @param a the first value
   * @param b the second value
   * @return 0 if the values are equal, &lt; 0 if a is less than b and &gt; 0 if b is greater than a
   */
  public int dupCompare(Txn txn, ByteBuffer a, ByteBuffer b) {
    txn.threadCheck();
    return Api.instance.mdb_dcmp(txn.txn, dbi, new Api.MDB_val(a), new Api.MDB_val(b));
  }
  
  /**
   * Empty/truncate the database
   * <p>
   * <code>mdb_drop(0)</code>
   * @param txn the transaction to use
   */
  public void truncateDatabase(Txn txn) {
    drop(txn, false);
  }
  
  /**
   * Delete and close the database
   * <p>
   * <code>mdb_drop(1)</code>
   * @param txn the transaction to use
   */
  public void deleteDatabase(Txn txn) {
    drop(txn, true);
  }
  
  /**
   * mdb_drop
   * @param txn the transaction to use
   * @param delete true to delete and close the database, false to only empty the database
   */
  public void drop(Txn txn, boolean delete) {
    txn.threadCheck();
    ApiErrors.checkError("mdb_drop", Api.instance.mdb_drop(txn.txn, dbi, delete ? 1 : 0));
  }
  
  /**
   * mdb_cursor_open
   * @param txn the transaction to use
   * @return the newly opened Cursor
   */
  public Cursor openCursor(Txn txn) {
    return new Cursor(txn, this);
  }
  
  /**
   * Close a database handle. Normally unnecessary. Use with care:
   *
   * This call is not mutex protected. Handles should only be closed by a
   * single thread, and only if no other threads are going to reference the 
   * database handle or one of its cursors any further. Do not close a handle 
   * if an existing transaction has modified its database. Doing so can cause 
   * misbehavior from database corruption to errors like MDB_BAD_VALSIZE 
   * (since the DB name is gone).
   *
   * Closing a database handle is not necessary, but lets mdb_dbi_open() reuse 
   * the handle value. Usually it's better to set a bigger 
   * mdb_env_set_maxdbs(), unless that value would be large.
   */
  public void close() {
    Api.instance.mdb_dbi_close(env.env, dbi);
  }
}
