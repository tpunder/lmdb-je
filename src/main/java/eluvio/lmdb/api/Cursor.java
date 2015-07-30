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

import java.nio.ByteBuffer;
import java.util.Objects;

import jnr.ffi.Pointer;
import jnr.ffi.TypeAlias;
import jnr.ffi.byref.NumberByReference;
import jnr.ffi.byref.PointerByReference;

/**
 * Cursors <b>MUST BE CLOSED</b> otherwise you will cause memory leaks
 * <p>
 * This cursor is only valid for use by the thread that created it and is
 * therefore <b>not thread-safe</b>.
 */
final public class Cursor implements AutoCloseable {
  public static class KeyAndData {
    public final ByteBuffer key;
    public final ByteBuffer data;
    
    protected KeyAndData(ByteBuffer key, ByteBuffer data) {
      this.key = key;
      this.data = data;
    }
  }
  
  private static enum State { OPEN, CLOSED }
  
  protected final Pointer cursor;
  private Txn txn;
  protected final DB db;
  private State state;

  /**
   * mdb_cursor_open
   * @param txn the transaction to use
   * @param db the database to use
   */
  public Cursor(Txn txn, DB db) {
    PointerByReference ref = new PointerByReference();
    ApiErrors.checkError("mdb_cursor_open", Api.instance.mdb_cursor_open(txn.txn, db.dbi, ref));
    cursor = ref.getValue();
    this.txn = txn;
    this.db = db;
    state = State.OPEN;
    txn.registerCursor(this);
  }
  
  /**
   * Position at specified key
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_SET} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public boolean moveTo(ByteBuffer key) { return null != get(key, null, CursorOp.MDB_SET); }
  
  /**
   * Position at specified key/value.  Only for {@link Api#MDB_DUPSORT}
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_GET_BOTH} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public boolean moveTo(ByteBuffer key, ByteBuffer value) { return null != get(key, value, CursorOp.MDB_GET_BOTH); }
  
  /**
   * Position at first key/data item
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_FIRST} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData first() { return get(CursorOp.MDB_FIRST); }
  
  /**
   * Position at last key/data item
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_LAST} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData last() { return get(CursorOp.MDB_LAST); }
  
  /**
   * Position at next data item
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_NEXT} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData next() { return get(CursorOp.MDB_NEXT); }
  
  /**
   * Position at previous data item
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_PREV} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData prev() { return get(CursorOp.MDB_PREV); }
  
  /**
   * Position at first data item of current key. Only for {@link Api#MDB_DUPSORT}
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_FIRST_DUP} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData firstDup() { return get(CursorOp.MDB_FIRST_DUP); }
  
  /**
   * Position at last data item of current key. Only for {@link Api#MDB_DUPSORT}
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_LAST_DUP} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData lastDup() { return get(CursorOp.MDB_LAST_DUP); }
  
  /**
   * Position at next data item of current key. Only for {@link Api#MDB_DUPSORT}
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_NEXT_DUP} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData nextDup() { return get(CursorOp.MDB_NEXT_DUP); }
  
  /**
   * Position at first data item of next key
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_NEXT_NODUP} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData nextNoDup() { return get(CursorOp.MDB_NEXT_NODUP); }
  
  /**
   * Position at previous data item of current key. Only for {@link Api#MDB_DUPSORT}
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_PREV_DUP} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData prevDup() { return get(CursorOp.MDB_PREV_DUP); }
  
  /**
   * Position at last data item of previous key
   * <p>
   * Equivalent to calling {@link #get} with {@link CursorOp#MDB_PREV_NODUP} as the parameter 
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData prevNoDup() { return get(CursorOp.MDB_PREV_NODUP); }
  
  /**
   * Position at first key greater than or equal to specified key.
   * @param key the key
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData ceiling(ByteBuffer key) {
    Objects.requireNonNull(key, "key ByteBuffer is null!");
    if (key.remaining() <= 0) throw new IllegalArgumentException("Key either null or has no data!");
    return get(key, null, CursorOp.MDB_SET_RANGE);
  }
  
  /**
   * Position at first key strictly greater than the specified key.
   * @param key the key
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor. 
   */
  public KeyAndData higher(ByteBuffer key) {
    final KeyAndData res = ceiling(key);
    if (null == res) return null;
    return 0 == db.compare(txn, key, res.key) ? nextNoDup() : res;
  }
  
  /**
   * Position at first key less than or equal to specified key.
   * @param key the key
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor. 
   */
  public KeyAndData floor(ByteBuffer key) {
    final KeyAndData res = ceiling(key);
    if (null == res) return null;
    return 0 == db.compare(txn, key, res.key) ? res : prevNoDup();
  }
  
  /**
   * Position at first key strictly less than the specified key.
   * @param key the key
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData lower(ByteBuffer key) {
    KeyAndData res = floor(key);
    if (null == res) return null;
    return 0 == db.compare(txn, key, res.key) ? prevNoDup() : res;
  }
  
//  /**
//   * Position at the exact key and closest dup value
//   * @param key the key
//   * @param value the value
//   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
//   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
//   *         is only valid until the next operation on the Cursor.
//   */
//  public KeyAndData closestDup(ByteBuffer key, ByteBuffer value) {
//    Objects.requireNonNull(key, "key ByteBuffer is null!");
//    Objects.requireNonNull(value, "value ByteBuffer is null!");
//    if (key.remaining() <= 0) throw new IllegalArgumentException("Key either null or has no data!");
//    if (value.remaining() <= 0) throw new IllegalArgumentException("Value either null or has no data!");
//    return get(key, value, CursorOp.MDB_GET_BOTH_RANGE);
//  }
  
  /**
   * Position at the exact key and dup value that is greater than or equal to specified value.
   * @param key the key
   * @param value the value
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData dupCeiling(ByteBuffer key, ByteBuffer value) {
    Objects.requireNonNull(key, "key ByteBuffer is null!");
    Objects.requireNonNull(value, "value ByteBuffer is null!");
    if (key.remaining() <= 0) throw new IllegalArgumentException("Key either null or has no data!");
    if (value.remaining() <= 0) throw new IllegalArgumentException("Value either null or has no data!");
    return get(key, value, CursorOp.MDB_GET_BOTH_RANGE);
  }
  
  /**
   * Position at exact key and first dup value strictly greater than the specified key.
   * @param key the key
   * @param value the value
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor. 
   */
  public KeyAndData dupHigher(ByteBuffer key, ByteBuffer value) {
    final KeyAndData res = dupCeiling(key, value);
    if (null == res) return null;
    final int cmp = db.dupCompare(txn, res.data, value);
    return cmp > 0 ? res : nextDup();
  }
  
  /**
   * Position at the exact key and first dup value less than or equal to specified value.
   * @param key the key
   * @param value the value
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor. 
   */
  public KeyAndData dupFloor(ByteBuffer key, ByteBuffer value) {
    final KeyAndData res = dupCeiling(key, value);
    if (null == res) return moveTo(key) ? lastDup() : null; 
    final int cmp = db.dupCompare(txn, res.data, value);
    return cmp <= 0 ? res : prevDup();
  }
  
  /**
   * Position at the exact key and first dup value strictly less than the specified value.
   * @param key the key
   * @param value the value
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData dupLower(ByteBuffer key, ByteBuffer value) {
    final KeyAndData res = dupCeiling(key, value);
    if (null == res) return moveTo(key) ? lastDup() : null;  
    final int cmp = db.dupCompare(txn, res.data, value);
    return cmp < 0 ? res : prevDup();
  }
  
  /**
   * mdb_cursor_get
   * @param op the LMDB CursorOp flag to use
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData get(CursorOp op) {
    return get(null, null, op);
  }
  
  /**
   * mdb_cursor_get
   * @param key the key param for mdb_cursor_get, or null if it is not needed
   * @param data the data param for mdb_cursor_get, or null if it is not needed
   * @param op the LMDB CursorOp flag to use
   * @return The matching KeyAndData from the underlying mdb_cursor_get call,
   *         or null if no match (MDB_NOTFOUND).  <b>Note:</b> the KeyAndData
   *         is only valid until the next operation on the Cursor.
   */
  public KeyAndData get(ByteBuffer key, ByteBuffer data, CursorOp op) {
    if (State.OPEN != state) throw new RuntimeException("Cursor is not open!");
    
    final Api.MDB_val keyVal = new Api.MDB_val(key);
    final Api.MDB_val dataVal = new Api.MDB_val(data);
    
    final int rc = Api.instance.mdb_cursor_get(cursor, keyVal, dataVal, op);
    
    // Don't throw an exception for this return code
    if (ApiErrors.MDB_NOTFOUND == rc) return null;
        
    ApiErrors.checkError("mdb_cursor_get", rc);
    
    return new KeyAndData(keyVal.asByteBuffer(), dataVal.asByteBuffer());
  }
  
  /**
   * mdb_cursor_put
   * @param key the key to store
   * @param data the data to store
   */
  public void put(ByteBuffer key, ByteBuffer data) {
    put(key, data);
  }
  
  /**
   * mdb_cursor_put
   * @param key the key to store
   * @param data the data to store
   * @param flags flags for the underlying mdb_cursor_put call
   */
  public void put(ByteBuffer key, ByteBuffer data, int flags) {
    if (State.OPEN != state) throw new RuntimeException("Cursor is not open!");
    if (key.remaining() < 1) throw new IllegalArgumentException("Key must be at least 1 byte long");
    
    ApiErrors.checkError("mdb_cursor_put", Api.instance.mdb_cursor_put(cursor, new Api.MDB_val(key), new Api.MDB_val(data), flags));
  }
  
  /**
   * An mdb_cursor_put with the MDB_RESERVE flag set
   * @param key the key
   * @param size the size of data to reserve
   * @return The data ByteBuffer that can be written into
   */
  public ByteBuffer reserve(ByteBuffer key, int size) {
    return reserve(key, size, 0);
  }
  
  /**
   * An mdb_cursor_put with the MDB_RESERVE flag set
   * @param key the key
   * @param size the size of data to reserve
   * @param flags flags (in addition to MDB_RESERVE) for the underlying mdb_cursor_put
   * @return The data ByteBuffer that can be written into
   */
  public ByteBuffer reserve(ByteBuffer key, int size, int flags) {
    if (State.OPEN != state) throw new RuntimeException("Cursor is not open!");
    if (key.remaining() < 1) throw new IllegalArgumentException("Key must be at least 1 byte long");
    
    final Api.MDB_val data = new Api.MDB_val(size);
    ApiErrors.checkError("mdb_cursor_put", Api.instance.mdb_cursor_put(cursor, new Api.MDB_val(key), data, flags | Api.MDB_RESERVE));
    
    return data.asByteBuffer();
  }
  
  /**
   * mdb_cursor_del
   */
  public void delete() {
    delete(0);
  }
  
  /**
   * mdb_cursor_del
   * @param flags flags to pass to the underlying mdb_cursor_del call
   */
  public void delete(int flags) {
    if (State.OPEN != state) throw new RuntimeException("Cursor is not open!");
    ApiErrors.checkError("mdb_cursor_del", Api.instance.mdb_cursor_del(cursor, flags));
  }
  
  public long keyCount() {
    if (State.OPEN != state) throw new RuntimeException("Cursor is not open!");
    
    final Api.MDB_val keyVal = new Api.MDB_val();
    final Api.MDB_val dataVal = new Api.MDB_val();
    
    long count = 0;
    
    int rc = Api.instance.mdb_cursor_get(cursor, keyVal, dataVal, CursorOp.MDB_FIRST);
    if (ApiErrors.MDB_NOTFOUND == rc) return 0;
    else count++;
    
    ApiErrors.checkError("mdb_cursor_get", rc); 
    
    while (true) {
      rc = Api.instance.mdb_cursor_get(cursor, keyVal, dataVal, CursorOp.MDB_NEXT_NODUP);
      if (0 == rc) count++;
      else if (ApiErrors.MDB_NOTFOUND == rc) break;
      else throw ApiErrors.toException("mdb_cursor_get", rc);
    }
    
    return count;
  }
  
  /**
   * mdb_cursor_count
   * <p>
   * Return count of duplicates for current key. 
   * <p>
   * <b>Only valid for {@link Api#MDB_DUPSORT} databases</b>
   * @return count of duplicates for current key.
   */
  public long dupCount() {
    if (State.OPEN != state) throw new RuntimeException("Cursor is not open!");
    final NumberByReference count = new NumberByReference(TypeAlias.size_t);
    ApiErrors.checkError("mdb_cursor_count", Api.instance.mdb_cursor_count(cursor, count));
    return count.longValue();
  }
  
  /**
   * mdb_cursor_renew
   * @param txn the txn to renew the cursor for
   */
  public void renew(Txn txn) {
    if (State.OPEN != state) throw new RuntimeException("Cursor has already been closed!");
    if (!this.txn.readOnly || !txn.readOnly) throw new RuntimeException("Can only call mdb_cursor_renew if the cursor is used with read-only transactions");
    ApiErrors.checkError("mdb_cursor_renew", Api.instance.mdb_cursor_renew(txn.txn, cursor));
    this.txn = txn;
  }
  
  /**
   * mdb_cursor_close
   */
  public void close() {
    if (State.CLOSED == state) return;
    Api.instance.mdb_cursor_close(cursor);
    state = State.CLOSED;
    txn.deregisterCursor(this);
    txn = null;
  }
}
