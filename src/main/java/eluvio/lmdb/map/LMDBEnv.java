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
package eluvio.lmdb.map;

public interface LMDBEnv extends AutoCloseable {
  /**
   * By default we set mdb_env_set_mapsize to 1TB which should be large enough
   * so that we don't have to worry about it.
   * <p>
   * This is just the maximum size that the database can grow to.
   */
  long DEFAULT_MAPSIZE = 1099511627776L; // 1TB

  /**
   * The maximum numbers of readers we can have accessing the DB
   */
  int DEFAULT_MAXREADERS = 4096;
  
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
  
  /**
   * Enable the MDB_NOMETASYNC flag
   */
  void disableMetaSync();
  
  /**
   * Disable the MDB_NOMETASYNC flag
   * 
   * Flush system buffers to disk only once per transaction, omit the metadata flush. Defer 
   * that until the system flushes files to disk, or next non-MDB_RDONLY commit or mdb_env_sync(). 
   * This optimization maintains database integrity, but a system crash may undo the last 
   * committed transaction. I.e. it preserves the ACI (atomicity, consistency, isolation) but 
   * not D (durability) database property.
   */
  void enableMetaSync();
  
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
  void disableSync();
  
  /**
   * Disable the MDB_NOSYNC flag
   */
  void enableSync();
}
