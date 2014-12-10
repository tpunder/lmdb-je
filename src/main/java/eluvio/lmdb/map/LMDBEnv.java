package eluvio.lmdb.map;

public interface LMDBEnv extends AutoCloseable {
  /**
   * By default we set mdb_env_set_mapsize to 1TB which should be large enough
   * so that we don't have to worry about it.
   * <p>
   * This is just the maximum size that the database can grow to.
   */
  final static long DEFAULT_MAPSIZE = 1099511627776L; // 1TB

  /**
   * Abort the current transaction for this thread
   */
  void abortTxn();

  /**
   * Begin a transaction. Transactions belong to the thread that created them
   * and only apply to operations for the current thread. There can only be a
   * single transaction per thread.
   */
  void beginTxn();

  /**
   * Begin a transaction. Transactions belong to the thread that created them
   * and only apply to operations for the current thread. There can only be a
   * single transaction per thread.
   * 
   * @param readOnly should this transaction be read-only?
   */
  void beginTxn(boolean readOnly);

  /**
   * Close the underlying LMDB Environment
   */
  void close();

  /**
   * Commit the current transaction for this thread
   */
  void commitTxn();

  /**
   * Is this a read-only environment?
   * 
   * @return true if this environment is read-only
   */
  boolean readOnly();

  LMDBTxn withExistingReadOnlyTxn();

  LMDBTxn withExistingReadWriteTxn();

  LMDBTxn withExistingTxn();

  LMDBTxn withNestedReadWriteTxn();

  LMDBTxn withReadOnlyTxn();
  
  LMDBTxn withReadWriteTxn();
}
