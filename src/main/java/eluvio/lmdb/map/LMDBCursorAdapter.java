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

import java.nio.ByteBuffer;
import java.util.Map;

abstract class LMDBCursorAdapter<T> implements AutoCloseable {
  public abstract T first();
  public abstract T last();
  public abstract T next();
  public abstract T prev();
  public abstract void delete();
  public abstract boolean readOnly();
  public abstract void close();
  
  public final LMDBCursorAdapter<T> reversed() {
    return new LMDBCursorAdapter<T>() {
      @Override public T first() { return LMDBCursorAdapter.this.last(); }
      @Override public T last() { return LMDBCursorAdapter.this.first(); }
      @Override public T next() { return LMDBCursorAdapter.this.prev(); }
      @Override public T prev() { return LMDBCursorAdapter.this.next(); }
      @Override public void delete() { LMDBCursorAdapter.this.delete(); }
      @Override public boolean readOnly() { return LMDBCursorAdapter.this.readOnly(); }
      @Override public void close() { LMDBCursorAdapter.this.close(); }
    };
  }
  
  public static <K,V> LMDBCursorAdapter<Map.Entry<K,V>> forEntries(final LMDBCursor<K,V> cursor) {
    return new LMDBCursorAdapter<Map.Entry<K,V>>() {
      public Map.Entry<K,V> first() { return cursor.first(); }
      public Map.Entry<K,V> last() { return cursor.last(); }
      public Map.Entry<K,V> next() { return cursor.next(); }
      public Map.Entry<K,V> prev() { return cursor.prev(); }
      public void delete() { cursor.delete(); }
      public boolean readOnly() { return cursor.readOnly(); }
      public void close() { cursor.close(); }
    };
  }
  
  public static <K> LMDBCursorAdapter<K> forKeys(final LMDBCursor<K,?> cursor) {
    return new LMDBCursorAdapter<K>() {
      @Override public K first() { return cursor.firstKey(); }
      @Override public K last() { return cursor.lastKey(); }
      @Override public K next() { return cursor.nextKey(); }
      @Override public K prev() { return cursor.prevKey(); }
      @Override public void delete() { cursor.delete(); }
      @Override public boolean readOnly() { return cursor.readOnly(); }
      @Override public void close() { cursor.close(); }
    };
  }
  
  public static <V> LMDBCursorAdapter<V> forValues(final LMDBCursor<?,V> cursor) {
    return new LMDBCursorAdapter<V>() {
      @Override public V first() { return cursor.firstValue(); }
      @Override public V last() { return cursor.lastValue(); }
      @Override public V next() { return cursor.nextValue(); }
      @Override public V prev() { return cursor.prevValue(); }
      @Override public void delete() { cursor.delete(); }
      @Override public boolean readOnly() { return cursor.readOnly(); }
      @Override public void close() { cursor.close(); }
    };
  }
  
  @SuppressWarnings("unchecked")
  public static <K,T> LMDBCursorAdapter<T> forDupValues(final LMDBCursor<K,T> cursor, final K key, final ByteBuffer keyBuf) {
    if (!cursor.moveTo(key, keyBuf)) {
      cursor.close();
      return (LMDBCursorAdapter<T>)empty;
    }
    
    return new LMDBCursorAdapter<T>() {
      @Override public T first() { return cursor.firstDupValue(); }
      @Override public T last() { return cursor.lastDupValue(); }
      @Override public T next() { return cursor.nextDupValue(); }
      @Override public T prev() { return cursor.prevDupValue(); }
      @Override public void delete() { cursor.delete(); }
      @Override public boolean readOnly() { return cursor.readOnly(); }
      @Override public void close() { cursor.close(); }
    };
  }
  
  public static final LMDBCursorAdapter<Object> empty = new LMDBCursorAdapter<Object>() {
    @Override public Object first() { return null; }
    @Override public Object last() { return null; }
    @Override public Object next() { return null; }
    @Override public Object prev() { return null; }
    @Override public void delete() { /* nothing to do */ }
    @Override public boolean readOnly() { return true; }
    @Override public void close() { }
  };
}
