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
import java.util.NoSuchElementException;

class LMDBIteratorImpl<T> implements LMDBIterator<T> {
  private final LMDBCursorAdapter<T> cursor;
  private T head = null;
  private boolean first = true;
  private boolean closed = false;
  
  public static <K,V> LMDBIterator<Map.Entry<K,V>> forEntries(LMDBMapInternal<K,V> map, LMDBCursor.Mode mode) {
    return forEntries(map.openCursor(mode));
  }
  
  public static <K,V> LMDBIterator<Map.Entry<K,V>> forEntries(LMDBCursor<K,V> cursor) {
    return new LMDBIteratorImpl<Map.Entry<K,V>>(LMDBCursorAdapter.forEntries(cursor));
  }
  
  public static <K> LMDBIterator<K> forKeys(LMDBMapInternal<K,?> map, LMDBCursor.Mode mode) {
    return forKeys(map.openCursor(mode));
  }
  
  public static <K> LMDBIterator<K> forKeys(LMDBCursor<K,?> cursor) {
    return new LMDBIteratorImpl<K>(LMDBCursorAdapter.forKeys(cursor));
  }
  
  public static <V> LMDBIterator<V> forValues(LMDBMapInternal<?,V> map, LMDBCursor.Mode mode) {
    return forValues(map.openCursor(mode));
  }
  
  public static <V> LMDBIterator<V> forValues(LMDBCursor<?,V> cursor) {
    return new LMDBIteratorImpl<V>(LMDBCursorAdapter.forValues(cursor));
  }
  
  public static <K,V> LMDBIterator<V> forDupValues(LMDBMapInternal<K,V> map, LMDBCursor.Mode mode, K key, ByteBuffer keyBuf) {
    return forDupValues(map.openCursor(mode), key, keyBuf);
  }
  
  public static <K,V> LMDBIterator<V> forDupValues(LMDBCursor<K,V> cursor, K key, ByteBuffer keyBuf) {
    return new LMDBIteratorImpl<V>(LMDBCursorAdapter.forDupValues(cursor, key, keyBuf));
  }
  
  public static <K,V> LMDBIterator<V> forDupValuesReversed(LMDBMapInternal<K,V> map, LMDBCursor.Mode mode, K key, ByteBuffer keyBuf) {
    return forDupValuesReversed(map.openCursor(mode), key, keyBuf);
  }
  
  public static <K,V> LMDBIterator<V> forDupValuesReversed(LMDBCursor<K,V> cursor, K key, ByteBuffer keyBuf) {
    return new LMDBIteratorImpl<V>(LMDBCursorAdapter.forDupValues(cursor, key, keyBuf).reversed());
  }
  
  LMDBIteratorImpl(LMDBCursorAdapter<T> cursor) {
    this.cursor = cursor;
  }
  
  public boolean hasNext() {
    if (closed) return false;
    
    if (null == head) {
      head = first ? cursor.first() : cursor.next();
      first = false;
      if (null == head) close();
    }
    
    return null != head;
  }
  
  public T next() {
    if (!hasNext()) throw new NoSuchElementException();
    final T res = head;
    head = null;
    return res;
  }
  
  public void remove() {
    if (cursor.readOnly()) {
      throw new UnsupportedOperationException("This cursor is using a read-only transaction therefore remove() is not supported.  Use a read-write transaction if you want remove() to work.");
    }
    
    cursor.delete();
  }
  
  public void close() {
    if (closed) return;
    cursor.close();
    closed = true;
  }
}

