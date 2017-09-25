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

import eluvio.lmdb.api.Api;
import eluvio.lmdb.api.Env;
import eluvio.lmdb.api.Txn;

/**
 * ReusableTxns must be closed
 */
final class ReusableTxn implements AutoCloseable {
  abstract class LMDBTxnImpl extends LMDBTxnInternal {
    @Override public void commit() { close(); }
    @Override public boolean readOnly() { return txn.readOnly; }
    @Override public Txn txn() { return txn; }
  }
  private final Env env;
  private volatile Txn txn = null;
  
  private int readOnlyDepth = 0;
  private final static int READ_ONLY = 1 << 0;
  private final static int ALLOW_NESTED = 1 << 1;
  private final static int REQUIRE_EXISTING = 1 << 2;
  private final static int USE_EXISTING_READ_OR_WRITE = 1 << 3;
  
  private final LMDBTxnInternal ReadWriteTxn = new LMDBTxnImpl() {
    public void abort() { txn.abort(); }
    public void close() {
      if (txn.isOpen()) txn.commit();
      txn = txn.parent;
      
      // If there was no parent txn and our readOnlyDepth > 0 then it means
      // this read-write txn was nested under 1 or more read-only transactions.
      // So we need to restore a read-only transaction.
      if (null == txn && readOnlyDepth > 0) txn = new Txn(env, Api.MDB_RDONLY);
    }
    
    public void commit() { txn.commit(); }
  };
  
  private final LMDBTxnInternal ReadOnlyTxn = new LMDBTxnImpl() {
    public void abort() { /* do nothing */ }
    public void close() {
      if (txn == null) throw new IllegalStateException("Expected txn to not be null");
		
      if (txn.isOpen()) {
        --readOnlyDepth;
        if (readOnlyDepth < 0) throw new IllegalStateException("readOnlyDepth is less than zero: "+readOnlyDepth);
        if (0 == readOnlyDepth) txn.reset();
      } else {
        throw new IllegalStateException("Expected read-only txn to still be open");
      }
    }
    public void commit() { /* do nothing */ }
    
    public Txn txn() { return txn; }
  };
  
  private final LMDBTxnInternal NopTxn = new LMDBTxnImpl() {
    public void abort() { /* Do nothing */ }
    public void close() { /* Do nothing */ }
    public void commit() { /* Do nothing */ }
    public Txn txn() { return txn; }
  };
  
  private final LMDBTxnInternal NonNestedReadWriteWithinReadWriteTxn = new LMDBTxnImpl() {
    public void abort() { throw new IllegalStateException("Cannot call abort() on a non nested read/write transaction!"); }
    public void close() { /* Do nothing */ }
    public void commit() { throw new IllegalStateException("Cannot call commit() on a non nested read/write transaction!"); }
    public Txn txn() { return txn; }
  };
    
  public ReusableTxn(Env env) {
    this.env = env;
  }
  
  public void abort() {
    if (!txn.readOnly && txn.isOpen()) txn.abort();
  }
  
  public void abortTxn() {
    if (null == txn) throw new IllegalStateException("No transaction to abort!");
    
    if (txn.readOnly) {
      txn.reset();
      --readOnlyDepth;
      if (0 != readOnlyDepth) throw new IllegalStateException("Expected readOnlyDepth to be 0");
    } else {
      txn.abort();
      txn = txn.parent;
    }
  }
  
  public LMDBTxnInternal beginTxn(boolean readOnly) {
    if (null != txn && txn.readOnly) if (txn.readOnly) throw new IllegalStateException("Cannot nest read-only transaction");
    return withTxn((readOnly ? READ_ONLY : 0) | ALLOW_NESTED);
  }
  
  /**
   * Aborts all outstanding transaction (including parent transactions)
   */
  public void close() {
    while (null != txn) {
      txn.abort();
      txn = txn.parent;
    }
    
    readOnlyDepth = 0;
  }

  public void commitTxn() {
    if (null == txn) throw new IllegalStateException("No transaction to commit!");
    
    if (txn.readOnly) {
      txn.reset();
      --readOnlyDepth;
      if (0 != readOnlyDepth) throw new IllegalStateException("Expected readOnlyDepth to be 0");
    } else {
      txn.commit();
      txn = txn.parent;
    }
  }

  /**
   * Since we store instances of this class in a ThreadLocal we *must* have a
   * finalizer to make sure we close out any outstanding transaction when this
   * class gets garbage collected due to the owning thread no longer existing.
   */
  @Override
  protected void finalize() {
    close();
  }

  public LMDBTxnInternal withExistingReadOnlyTxn() {
    return withTxn(READ_ONLY | REQUIRE_EXISTING);
  }
  
  public LMDBTxnInternal withExistingReadWriteTxn() {
    return withTxn(REQUIRE_EXISTING);
  }

  public LMDBTxnInternal withExistingTxn() {
    return withTxn(USE_EXISTING_READ_OR_WRITE);
  }
  
  public LMDBTxnInternal withNestedReadWriteTxn() {
    return withTxn(ALLOW_NESTED);
  }
  
  public LMDBTxnInternal withReadOnlyTxn() {
    return withTxn(READ_ONLY);
  }
  
  public LMDBTxnInternal withReadWriteTxn() {
    return withTxn();
  }
  
  private LMDBTxnInternal withTxn() {
    return withTxn(0);
  }
  
  private LMDBTxnInternal withTxn(int flags) {
    assert (flags & (READ_ONLY | ALLOW_NESTED | REQUIRE_EXISTING | USE_EXISTING_READ_OR_WRITE)) == flags;
    
    final boolean readOnly = (flags & READ_ONLY) == READ_ONLY;
    final boolean allowNested = (flags & ALLOW_NESTED) == ALLOW_NESTED;
    final boolean requireExisting = (flags & REQUIRE_EXISTING) == REQUIRE_EXISTING;
    final boolean useExistingReadOrWrite = (flags & USE_EXISTING_READ_OR_WRITE) == USE_EXISTING_READ_OR_WRITE;
    
    // This is an exclusive flag
    if (useExistingReadOrWrite) assert flags == USE_EXISTING_READ_OR_WRITE;
    
    // No existing transaction, so start one of the requested type
    if (null == txn) {
      if (requireExisting || useExistingReadOrWrite) throw new IllegalStateException("Expected an existing transaction but none was found!");
      
      txn = new Txn(env, readOnly ? Api.MDB_RDONLY : 0);
      readOnlyDepth = readOnly ? 1 : 0;
      return readOnly ? ReadOnlyTxn : ReadWriteTxn;
    }
    
    if (useExistingReadOrWrite) {
      if (!txn.isOpen()) throw new IllegalStateException("Expected an existing open transaction but none was found!");
      return NopTxn;
    }
    
    if (readOnly) {
      // We want a read-only txn and we already have a read-only txn
      if (txn.readOnly) {
        if (requireExisting) {
          if (!txn.isOpen()) throw new IllegalStateException("Found existing read-only txn but it is not currently open");
          return NopTxn;
        } else {
          ++readOnlyDepth;
          if (txn.isInit()) txn.renew();
          return ReadOnlyTxn;
        }
      } else {
        // Otherwise we want a read-only but have a read-write.
        // In this case we do nothing and just use the existing
        // read-write transaction and the close() method becomes
        // a no-op.
        return NopTxn;
      }
    }
    
    // If we get this far then we want a read-write txn
    
    if (txn.readOnly) {      
      if (txn.isOpen()) {
        throw new IllegalStateException("A read/write transaction was requested but a read-only transaction is already open!");
      }

      // We have an read-only transaction in the INIT state that needs
      // to be aborted so we can re-open as read-write
      txn.abort();
      txn = new Txn(env);
      return ReadWriteTxn;
    } else {
      // Want a read/write txn
      if (allowNested) {
        // We create a nested transaction
        txn = new Txn(env, txn);
        return ReadWriteTxn;
      } else {
        return NonNestedReadWriteWithinReadWriteTxn;
      }
    }
  }
}
