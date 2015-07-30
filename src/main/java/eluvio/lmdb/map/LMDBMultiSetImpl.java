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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

class LMDBMultiSetImpl<K, V> extends LMDBMultiSetInternal<V> {

  final LMDBMapInternal<K, V> map;

  private final K key;

  private final ByteBuffer keyBuf;

  LMDBMultiSetImpl(LMDBMapInternal<K, V> map, K key) {
    this(map, key, map.keySerializer().serialize(key, null));
  }

  LMDBMultiSetImpl(LMDBMapInternal<K, V> map, K key, ByteBuffer keyBuf) {
    this.map = map;
    this.key = key;
    this.keyBuf = keyBuf;
  }

  @Override
  public boolean add(V value) {
    return map.add(key, keyBuf, value);
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    boolean modified = false;

    try (LMDBTxnInternal txn = withReadWriteTxn()) {
      for (V v : c) {
        if (add(v)) modified = true;
      }
    }

    return modified;
  }

  @Override
  V ceiling(ByteBuffer valueBuf) {
    return map.dupCeiling(key, keyBuf, valueBuf);
  }

  @Override
  public V ceiling(V v) {
    return map.dupCeiling(key, keyBuf, v);
  }

  @Override
  public void clear() {
    map.removeNoPrev(key, keyBuf);
  }

  @Override
  public void close() {
    // Nothing to do -- we don't need to close the LMDB database
  }

  @Override
  public Comparator<? super V> comparator() {
    return map.valueComparator();
  }

  @Override
  public int compare(V a, ByteBuffer aBuf, V b, ByteBuffer bBuf) {
    return map.valueCompare(a, aBuf, b, bBuf);
  }

  @Override
  public int compare(V a, V b) {
    return map.valueCompare(a, b);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean contains(Object value) {
    return map.contains(key, keyBuf, (V) value);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    try (LMDBTxnInternal txn = withReadOnlyTxn()) {
      for (Object v : c) {
        if (!contains(v)) return false;
      }
    }

    return true;
  }

  @Override
  public Iterator<V> descendingIterator() {
    return LMDBIteratorImpl.forDupValuesReversed(map, LMDBCursor.Mode.USE_EXISTING_TXN, key, keyBuf);
  }
  
  @Override
  public LMDBIterator<V> descendingLMDBIterator() {
    return LMDBIteratorImpl.forDupValuesReversed(map, LMDBCursor.Mode.READ_ONLY, key, keyBuf);
  }

  @Override
  public LMDBMultiSet<V> descendingSet() {
    return new LMDBMultiSetReversed<V>(this);
  }

  @Override
  LMDBEnvInternal env() {
    return map.env();
  }

  @Override
  public V first() {
    try (LMDBCursor<K, V> cursor = map.openReadOnlyCursor()) {
      return cursor.moveTo(key, keyBuf) ? cursor.firstDupValue() : null;
    }
  }

  @Override
  V floor(ByteBuffer valueBuf) {
    return map.dupFloor(key, keyBuf, valueBuf);
  }

  @Override
  public V floor(V v) {
    return map.dupFloor(key, keyBuf, v);
  }

  @Override
  public LMDBMultiSet<V> headSet(V toElement) {
    return headSet(toElement, false);
  }

  @Override
  public LMDBMultiSet<V> headSet(V toElement, boolean inclusive) {
    return LMDBMultiSetView.headSet(this, toElement, inclusive);
  }

  @Override
  V higher(ByteBuffer valueBuf) {
    return map.dupHigher(key, keyBuf, valueBuf);
  }

  @Override
  public V higher(V v) {
    return map.dupHigher(key, keyBuf, v);
  }
  
  @Override
  public boolean isEmpty() {
    return null == first();
  }

  @Override
  public Iterator<V> iterator() {
    return LMDBIteratorImpl.forDupValues(map, LMDBCursor.Mode.USE_EXISTING_TXN, key, keyBuf);
  }

  @Override
  public V last() {
    try (LMDBCursor<K, V> cursor = map.openReadOnlyCursor()) {
      return cursor.moveTo(key, keyBuf) ? cursor.lastDupValue() : null;
    }
  }

  @Override
  public LMDBIterator<V> lmdbIterator() {
    return LMDBIteratorImpl.forDupValues(map, LMDBCursor.Mode.READ_ONLY, key, keyBuf);
  }

  @Override
  V lower(ByteBuffer valueBuf) {
    return map.dupLower(key, keyBuf, valueBuf);
  }

  @Override
  public V lower(V v) {
    return map.dupLower(key, keyBuf, v);
  }

  @Override
  public V pollFirst() {
    try (LMDBCursor<K, V> cursor = map.openReadWriteCursor()) {
      if (cursor.moveTo(key, keyBuf)) {
        final V v = cursor.firstDupValue();
        if (null != v) cursor.delete();
        return v;
      }

      return null;
    }
  }

  @Override
  public V pollLast() {
    try (LMDBCursor<K, V> cursor = map.openReadWriteCursor()) {
      if (cursor.moveTo(key, keyBuf)) {
        final V v = cursor.lastDupValue();
        if (null != v) cursor.delete();
        return v;
      }

      return null;
    }
  }
  
  @SuppressWarnings("unchecked")
  MultiSetCursor<V> openCursor(LMDBCursor.Mode mode) {
    final LMDBCursor<K,V> cursor = map.openCursor(mode);
    
    if (!cursor.moveTo(key, keyBuf)) {
      cursor.close();
      return (MultiSetCursor<V>)EmptyMultiSetCursor;
    }
    
    return new MultiSetCursorImpl(cursor);
  }
  
  static abstract class MultiSetCursor<T> extends LMDBCursorAdapter<T> {
    abstract T ceiling(ByteBuffer valueBuf);
    abstract T floor(ByteBuffer valueBuf);
    abstract T lower(ByteBuffer valueBuf);
    abstract T higher(ByteBuffer valueBuf);
  }
  
  private class MultiSetCursorImpl extends MultiSetCursor<V> {
    private final LMDBCursor<K,V> cursor;
    
    MultiSetCursorImpl(LMDBCursor<K,V> cursor) {
      this.cursor = cursor;
    }

    @Override public V first() { return cursor.firstDupValue(); }
    @Override public V last() { return cursor.lastDupValue(); }
    @Override public V next() { return cursor.nextDupValue(); }
    @Override public V prev() { return cursor.prevDupValue(); }
    @Override public void delete() { cursor.delete(); }
    @Override public boolean readOnly() { return cursor.readOnly(); }
    @Override public void close() { cursor.close(); }
    @Override V ceiling(ByteBuffer valueBuf) { return cursor.dupCeiling(keyBuf, valueBuf); }
    @Override V floor(ByteBuffer valueBuf) { return cursor.dupFloor(keyBuf, valueBuf); }
    @Override V higher(ByteBuffer valueBuf) { return cursor.dupHigher(keyBuf, valueBuf); }
    @Override V lower(ByteBuffer valueBuf) { return cursor.dupLower(keyBuf, valueBuf); }
  }
  
  private static final MultiSetCursor<Object> EmptyMultiSetCursor = new MultiSetCursor<Object>() {
    @Override public Object first() { return null; }
    @Override public Object last() { return null; }
    @Override public Object next() { return null; }
    @Override public Object prev() { return null; }
    @Override public void delete() {  }
    @Override public boolean readOnly() { return true; }
    @Override public void close() { }
    @Override Object ceiling(ByteBuffer valueBuf) { return null; }
    @Override Object floor(ByteBuffer valueBuf) { return null; }
    @Override Object higher(ByteBuffer valueBuf) { return null; }
    @Override Object lower(ByteBuffer valueBuf) { return null; }
  };

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    return map.remove(key, keyBuf, (V) o);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;

    try (LMDBTxnInternal txn = map.withReadWriteTxn()) {
      for (Object v : c) {
        if (remove(v)) modified = true;
      }
    }

    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public int size() {
    try (LMDBCursor<K, V> cursor = map.openReadOnlyCursor()) {
      if (!cursor.moveTo(key, keyBuf)) return 0;

      long c = cursor.dupCount();
      if (c > Integer.MAX_VALUE) throw new RuntimeException("Integer Overflow: " + c);
      return (int) c;
    }
  }

  @Override
  public LMDBMultiSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive) {
    return LMDBMultiSetView.subSet(this, fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public LMDBMultiSet<V> subSet(V fromElement, V toElement) {
    return subSet(fromElement, true, toElement, false);
  }
  
  @Override
  public LMDBMultiSet<V> tailSet(V fromElement) {
    return tailSet(fromElement, true);
  }

  @Override
  public LMDBMultiSet<V> tailSet(V fromElement, boolean inclusive) {
    return LMDBMultiSetView.tailSet(this, fromElement, inclusive);
  }

  @Override
  LMDBSerializer<V> valueSerializer() {
    return map.valueSerializer();
  }
  
  @Override
  public void disableMetaSync() {
    map.disableMetaSync();
  }
  
  @Override
  public void enableMetaSync() {
    map.enableMetaSync();
  }
  
  @Override
  public void disableSync() {
    map.disableSync();
  }
  
  @Override
  public void enableSync() {
    map.enableSync();
  }
}
