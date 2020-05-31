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
import java.util.Comparator;
import java.util.Map;

class LMDBMapReversed<K, V> extends LMDBMapInternal<K, V> {
  private class CursorImpl implements LMDBCursor<K, V> {
    private final LMDBCursor<K, V> parent;

    public CursorImpl(LMDBCursor<K, V> parent) {
      this.parent = parent;
    }

    @Override
    public void close() {
      parent.close();
    }
    
    @Override
    public void delete() {
      parent.delete();
    }

    @Override
    public V dupCeiling(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return parent.dupFloor(keyBuf, valueBuf);
    }

    @Override
    public long dupCount() {
      return parent.dupCount();
    }

    @Override
    public V dupFloor(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return parent.dupCeiling(keyBuf, valueBuf);
    }

    @Override
    public V dupHigher(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return parent.dupLower(keyBuf, valueBuf);
    }

    @Override
    public V dupLower(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return parent.dupHigher(keyBuf, valueBuf);
    }

    @Override
    public Map.Entry<K, V> first() {
      return parent.last();
    }

    @Override
    public V firstDupValue() {
      return parent.lastDupValue();
    }

    @Override
    public K firstKey() {
      return parent.lastKey();
    }

    @Override
    public V firstValue() {
      return parent.lastValue();
    }

    @Override
    public Map.Entry<K, V> last() {
      return parent.first();
    }

    @Override
    public V lastDupValue() {
      return parent.firstDupValue();
    }

    @Override
    public K lastKey() {
      return parent.firstKey();
    }

    @Override
    public V lastValue() {
      return parent.firstValue();
    }

    @Override
    public boolean moveTo(K key, ByteBuffer keyBuf) {
      return parent.moveTo(key, keyBuf);
    }

    @Override
    public boolean moveTo(K key, ByteBuffer keyBuf, V value, ByteBuffer valueBuf) {
      return parent.moveTo(key, keyBuf, value, valueBuf);
    }

    @Override
    public Map.Entry<K, V> next() {
      return parent.prev();
    }

    @Override
    public V nextDupValue() {
      return parent.prevDupValue();
    }

    @Override
    public K nextKey() {
      return parent.prevKey();
    }

    @Override
    public V nextValue() {
      return parent.prevValue();
    }
    
    @Override
    public Map.Entry<K, V> prev() {
      return parent.next();
    }
    
    @Override
    public V prevDupValue() {
      return parent.nextDupValue();
    }
    
    @Override
    public K prevKey() {
      return parent.nextKey();
    }
    
    @Override
    public V prevValue() {
      return parent.nextValue();
    }

    @Override
    public boolean readOnly() {
      return parent.readOnly();
    }
  }

  private final LMDBMapInternal<K, V> map;

  private final LMDBKeySet<K> keySet;

  private final LMDBEntrySet<K, V> entrySet;

  private final LMDBValuesCollection<V> values;

  LMDBMapReversed(LMDBMapInternal<K, V> map) {
    this.map = map;
    keySet = new LMDBKeySet<K>(this);
    entrySet = new LMDBEntrySet<K, V>(this);
    values = new LMDBValuesCollection<V>(this);
  }

  @Override
  boolean add(K key, ByteBuffer keyBuf, V value) {
    return map.add(key, keyBuf, value);
  }

  @Override
  public boolean add(K key, V value) {
    return map.add(key, value);
  }

  @Override
  public boolean append(K key, V value) {
    map.putNoPrev(key, value);
    return true;
  }

  @Override
  public Map.Entry<K, V> ceilingEntry(K key) {
    return map.floorEntry(key);
  }

  @Override
  public K ceilingKey(K key) {
    return map.floorKey(key);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public void close() {
    map.close();
  }

  @Override
  public Comparator<? super K> comparator() {
    return this;
  }

  @Override
  public int compare(K a, ByteBuffer aBuf, K b, ByteBuffer bBuf) {
    return -1 * map.compare(a, aBuf, b, bBuf);
  }

  @Override
  public int compare(K a, K b) {
    return -1 * map.compare(a, b);
  }

  @Override
  boolean contains(K key, ByteBuffer keyBuf, V value) {
    return map.contains(key, keyBuf, value);
  }

  @Override
  public boolean contains(K key, V value) {
    return map.contains(key, value);
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public LMDBKeySet<K> descendingKeySet() {
    return map.keySet();
  }

  @Override
  public LMDBMapInternal<K, V> descendingMap() {
    return map;
  }

  @Override
  boolean dup() {
    return map.dup();
  }

  @Override
  V dupCeiling(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return map.dupFloor(key, keyBuf, valueBuf);
  }

  @Override
  V dupCeiling(K key, ByteBuffer keyBuf, V value) {
    return map.dupFloor(key, keyBuf, value);
  }

  @Override
  V dupFloor(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return map.dupCeiling(key, keyBuf, valueBuf);
  }

  @Override
  V dupFloor(K key, ByteBuffer keyBuf, V value) {
    return map.dupCeiling(key, keyBuf, value);
  }

  @Override
  V dupHigher(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return map.dupLower(key, keyBuf, valueBuf);
  }

  @Override
  V dupHigher(K key, ByteBuffer keyBuf, V value) {
    return map.dupLower(key, keyBuf, value);
  }

  @Override
  V dupLower(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return map.dupHigher(key, keyBuf, valueBuf);
  }

  @Override
  V dupLower(K key, ByteBuffer keyBuf, V value) {
    return map.dupHigher(key, keyBuf, value);
  }

  @Override
  public LMDBEntrySet<K, V> entrySet() {
    return entrySet;
  }

  @Override
  LMDBEnvInternal env() {
    return map.env();
  }

  @Override
  public Map.Entry<K, V> firstEntry() {
    return map.lastEntry();
  }

  @Override
  public K firstKey() {
    return map.lastKey();
  }

  @Override
  public Map.Entry<K, V> floorEntry(K key) {
    return map.ceilingEntry(key);
  }

  @Override
  public K floorKey(K key) {
    return map.ceilingKey(key);
  }

  @Override
  public V get(Object key) {
    return map.get(key);
  }

  @Override
  public LMDBMapInternal<K, V> headMap(K toKey) {
    return map.tailMap(toKey).descendingMap();
  }

  @Override
  public LMDBMapInternal<K, V> headMap(K toKey, boolean toInclusive) {
    return map.tailMap(toKey, toInclusive).descendingMap();
  }

  @Override
  public Map.Entry<K, V> higherEntry(K key) {
    return map.lowerEntry(key);
  }

  @Override
  public K higherKey(K key) {
    return map.lowerKey(key);
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public long keyCount() {
    return map.keyCount();
  }

  @Override
  LMDBSerializer<K> keySerializer() {
    return map.keySerializer();
  }

  @Override
  public LMDBKeySet<K> keySet() {
    return keySet;
  }

  @Override
  public Map.Entry<K, V> lastEntry() {
    return map.firstEntry();
  }

  @Override
  public K lastKey() {
    return map.firstKey();
  }

  @Override
  public Map.Entry<K, V> lowerEntry(K key) {
    return map.higherEntry(key);
  }

  @Override
  public K lowerKey(K key) {
    return map.higherKey(key);
  }

  @Override
  public LMDBKeySet<K> navigableKeySet() {
    return keySet;
  }

  @Override
  protected LMDBCursor<K, V> openCursor(LMDBCursor.Mode mode) {
    return new CursorImpl(map.openCursor(mode));
  }

  @Override
  public Map.Entry<K, V> pollFirstEntry() {
    return map.pollLastEntry();
  }

  @Override
  public K pollFirstKey() {
    return map.pollLastKey();
  }

  @Override
  public Map.Entry<K, V> pollLastEntry() {
    return map.pollFirstEntry();
  }

  @Override
  public K pollLastKey() {
    return map.pollFirstKey();
  }

  @Override
  public boolean prepend(K key, V value) {
    return map.append(key, value);
  }

  @Override
  public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    map.putAll(m);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return map.putIfAbsent(key, value);
  }

  @Override
  public void putNoPrev(K key, V value) {
    map.putNoPrev(key, value);
  }

  @Override
  boolean remove(K key, ByteBuffer keyBuf, V value) {
    return map.remove(key, keyBuf, value);
  }

  @Override
  public V remove(Object key) {
    return map.remove(key);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return map.remove(key, value);
  }

  @Override
  public boolean removeNoPrev(K key) {
    return map.removeNoPrev(key);
  }

  @Override
  boolean removeNoPrev(K key, ByteBuffer keyBuf) {
    return map.removeNoPrev(key, keyBuf);
  }

  @Override
  public V replace(K key, V value) {
    return map.replace(key, value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return map.replace(key, oldValue, newValue);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public LMDBMapInternal<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return map.subMap(toKey, toInclusive, fromKey, fromInclusive).descendingMap();
  }

  @Override
  public LMDBMapInternal<K, V> subMap(K fromKey, K toKey) {
    // This differs from the normal implementation since the
    // fromInclusive/toInclusive defaults are swapped
    return map.subMap(toKey, false, fromKey, true).descendingMap();
  }

  @Override
  public LMDBMapInternal<K, V> tailMap(K fromKey) {
    return map.headMap(fromKey).descendingMap();
  }

  @Override
  public LMDBMapInternal<K, V> tailMap(K fromKey, boolean fromInclusive) {
    return map.headMap(fromKey, fromInclusive).descendingMap();
  }

  @Override
  Comparator<V> valueComparator() {
    return map.valueComparator();
  }

  @Override
  int valueCompare(V a, ByteBuffer aBuf, V b, ByteBuffer bBuf) {
    return map.valueCompare(a, aBuf, b, bBuf);
  }

  @Override
  int valueCompare(V a, V b) {
    return map.valueCompare(a, b);
  }

  @Override
  public long valueCount() {
    return map.valueCount();
  }

  @Override
  public LMDBValuesCollection<V> values() {
    return values;
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
  public void disableSync() { map.disableSync(); }
  
  @Override
  public void enableSync() {
    map.enableSync();
  }

  @Override
  public void sync() { map.sync(); }

  @Override
  public void sync(boolean force) { map.sync(force); }
}
