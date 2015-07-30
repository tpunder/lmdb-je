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
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

class LMDBMapView<K, V> extends LMDBMapInternal<K, V> {
  private static enum CeilingMode {
    NoPossibleMatch, UseHigher, UseCeiling
  }

  private class CursorImpl implements LMDBCursor<K, V> {
    private final LMDBMapImpl<K, V>.CursorImpl parent;

    public CursorImpl(LMDBMapImpl<K, V>.CursorImpl parent) {
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
      return parent.dupCeiling(keyBuf, valueBuf);
    }

    @Override
    public long dupCount() {
      return parent.dupCount();
    }

    @Override
    public V dupFloor(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return parent.dupFloor(keyBuf, valueBuf);
    }

    @Override
    public V dupHigher(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return parent.dupHigher(keyBuf, valueBuf);
    }

    @Override
    public V dupLower(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return parent.dupLower(keyBuf, valueBuf);
    }

    @Override
    public Map.Entry<K, V> first() {
      if (null == fromKey) return parent.first();
      final Map.Entry<K, V> entry = fromInclusive ? parent.ceiling(fromKeyBuf) : parent.higher(fromKeyBuf);
      if (null != entry && !withinRange(entry.getKey())) return null;
      return entry;
    }

    @Override
    public V firstDupValue() {
      return parent.firstDupValue();
    }

    @Override
    public K firstKey() {
      if (null == fromKey) return parent.firstKey();
      final K key = fromInclusive ? parent.ceilingKey(fromKeyBuf) : parent.higherKey(fromKeyBuf);
      if (null != key && !withinRange(key)) return null;
      return key;
    }

    @Override
    public V firstValue() {
      final Map.Entry<K, V> entry = first();
      return null != entry ? entry.getValue() : null;
    }

    @Override
    public Map.Entry<K, V> last() {
      if (null == toKey) return parent.last();
      final Map.Entry<K, V> entry = toInclusive ? parent.floor(toKeyBuf) : parent.lower(toKeyBuf);
      if (null != entry && !withinRange(entry.getKey())) return null;
      return dup() ? new AbstractMap.SimpleImmutableEntry<K,V>(entry.getKey(), parent.lastDupValue()) : entry;
    }

    @Override
    public V lastDupValue() {
      return parent.lastDupValue();
    }

    @Override
    public K lastKey() {
      if (null == toKey) return parent.lastKey();
      final K res = toInclusive ? parent.floorKey(toKeyBuf) : parent.lowerKey(toKeyBuf);
      if (null != res && !withinRange(res)) return null;
      return res;
    }

    @Override
    public V lastValue() {
      final Map.Entry<K, V> entry = last();
      return null != entry ? entry.getValue() : null;
    }

    @Override
    public boolean moveTo(K key, ByteBuffer keyBuf) {
      return withinRange(key, keyBuf) ? parent.moveTo(key, keyBuf) : false;
    }

    @Override
    public boolean moveTo(K key, ByteBuffer keyBuf, V value, ByteBuffer valueBuf) {
      return withinRange(key, keyBuf) ? parent.moveTo(key, keyBuf, value, valueBuf) : false;
    }
    
    @Override
    public Map.Entry<K, V> next() {
      final Map.Entry<K, V> entry = parent.next();
      if (null != entry && !withinRange(entry.getKey())) return null;
      return entry;
    }
    
    @Override
    public V nextDupValue() {
      return parent.nextDupValue();
    }
    
    @Override
    public K nextKey() {
      final K key = parent.nextKey();
      if (null != key && !withinRange(key)) return null;
      return key;
    }
    
    @Override
    public V nextValue() {
      final Map.Entry<K, V> entry = next();
      return null != entry ? entry.getValue() : null;
    }

    @Override
    public Map.Entry<K, V> prev() {
      final Map.Entry<K, V> entry = parent.prev();
      if (null != entry && !withinRange(entry.getKey())) return null;
      return entry;
    }

    @Override
    public V prevDupValue() {
      return parent.prevDupValue();
    }

    @Override
    public K prevKey() {
      final K key = parent.prevKey();
      if (null != key && !withinRange(key)) return null;
      return key;
    }

    @Override
    public V prevValue() {
      final Map.Entry<K, V> entry = prev();
      return null != entry ? entry.getValue() : null;
    }

    @Override
    public boolean readOnly() {
      return parent.readOnly();
    }
  }

  private static enum FloorMode {
    NoPossibleMatch, UseLower, UseFloor
  }

  public static <K, V> LMDBMapView<K, V> headMap(LMDBMapImpl<K, V> map, K toKey, boolean toInclusive) {
    return new LMDBMapView<K, V>(map, null, false, toKey, toInclusive);
  }

  public static <K, V> LMDBMapView<K, V> subMap(LMDBMapImpl<K, V> map, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return new LMDBMapView<K, V>(map, fromKey, fromInclusive, toKey, toInclusive);
  }

  public static <K, V> LMDBMapView<K, V> tailMap(LMDBMapImpl<K, V> map, K fromKey, boolean fromInclusive) {
    return new LMDBMapView<K, V>(map, fromKey, fromInclusive, null, false);
  }

  private final LMDBMapImpl<K, V> map;
  private final K fromKey;
  private final boolean fromInclusive;
  private final K toKey;
  private final boolean toInclusive;
  private final ByteBuffer fromKeyBuf;
  private final ByteBuffer toKeyBuf;
  private final LMDBMapInternal<K, V> reversed;

  private final LMDBKeySet<K> keySet;

  private final LMDBEntrySet<K, V> entrySet;

  private final LMDBValuesCollection<V> values;

  public LMDBMapView(LMDBMapImpl<K, V> map, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    if (null == map) throw new IllegalArgumentException("Missing Map");
    if (null == fromKey && null == toKey) throw new IllegalArgumentException("fromKey and toKey cannot both be null!");

    if (null != fromKey && null != toKey && map.compare(fromKey, toKey) >= 0) throw new IllegalArgumentException("Expected fromKey to be less than toKey");

    this.map = map;
    this.fromKey = fromKey;
    this.fromInclusive = fromInclusive;
    this.toKey = toKey;
    this.toInclusive = toInclusive;

    fromKeyBuf = null != fromKey ? map.keySerializer.serialize(fromKey, null) : null;
    toKeyBuf = null != toKey ? map.keySerializer.serialize(toKey, null) : null;

    reversed = new LMDBMapReversed<K, V>(this);
    keySet = new LMDBKeySet<K>(this);
    entrySet = new LMDBEntrySet<K, V>(this);
    values = new LMDBValuesCollection<V>(this);
  }

  @Override
  boolean add(K key, ByteBuffer keyBuf, V value) {
    rangeCheck(key);
    return map.add(key, keyBuf, value);
  }

  @Override
  public boolean add(K key, V value) {
    rangeCheck(key);
    return map.add(key, value);
  }

  /**
   * @return fromKey if key &lt; fromKey, toKey if key &gt; toKey, otherwise key
   *         unchanged
   */
  protected K adjustKey(K key) {
    if (null != fromKey && compare(key, null, fromKey, fromKeyBuf) < 0) return fromKey;
    if (null != toKey && compare(key, null, toKey, toKeyBuf) > 0) return toKey;
    return key;
  }

  @Override
  public boolean append(K key, V value) {
    rangeCheck(key);
    return map.append(key, value);
  }

  @Override
  public Map.Entry<K, V> ceilingEntry(K key) {
    key = adjustKey(key);

    switch (ceilingMode(key)) {
      case NoPossibleMatch:
        return null;
      case UseHigher:
        return handleNavigableEntryResult(map.higherEntry(key));
      case UseCeiling:
        return handleNavigableEntryResult(map.ceilingEntry(key));
      default:
        throw new IllegalStateException("Unknown Enum Value");
    }
  }

  @Override
  public K ceilingKey(K key) {
    key = adjustKey(key);

    switch (ceilingMode(key)) {
      case NoPossibleMatch:
        return null;
      case UseHigher:
        return handleNavigableKeyResult(map.higherKey(key));
      case UseCeiling:
        return handleNavigableKeyResult(map.ceilingKey(key));
      default:
        throw new IllegalStateException("Unknown Enum Value");
    }
  }

  private CeilingMode ceilingMode(K key) {
    if (!toInclusive && Objects.equals(key, toKey)) return CeilingMode.NoPossibleMatch;
    if (!fromInclusive && Objects.equals(key, fromKey)) return CeilingMode.UseHigher;
    return CeilingMode.UseCeiling;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not currently supported for submaps");
  }

  @Override
  public void close() {
    map.close();
  }

  @Override
  public Comparator<K> comparator() {
    return this;
  }

  @Override
  public int compare(K a, ByteBuffer aBuf, K b, ByteBuffer bBuf) {
    return map.compare(a, aBuf, b, bBuf);
  }

  @Override
  public int compare(K a, K b) {
    return map.compare(a, b);
  }

  @Override
  boolean contains(K key, ByteBuffer keyBuf, V value) {
    return withinRange(key, keyBuf) && map.contains(key, keyBuf, value);
  }

  @Override
  public boolean contains(K key, V value) {
    return withinRange(key) && map.contains(key, value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean containsKey(Object key) {
    if (!withinRange((K) key)) return false;
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    try (LMDBIterator<Map.Entry<K, V>> it = entrySet().lmdbIterator()) {
      while (it.hasNext()) {
        final Map.Entry<K, V> entry = it.next();
        if (Objects.equals(value, entry.getValue())) return true;
      }
    }

    return false;
  }

  @Override
  public LMDBKeySet<K> descendingKeySet() {
    return descendingMap().keySet();
  }

  @Override
  public LMDBMapInternal<K, V> descendingMap() {
    return reversed;
  }

  @Override
  boolean dup() {
    return map.dup();
  }

  @Override
  V dupCeiling(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return withinRange(key) ? map.dupCeiling(key, keyBuf, valueBuf) : null;
  }

  @Override
  V dupCeiling(K key, ByteBuffer keyBuf, V value) {
    return withinRange(key) ? map.dupCeiling(key, keyBuf, value) : null;
  }

  @Override
  V dupFloor(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return withinRange(key) ? map.dupFloor(key, keyBuf, valueBuf) : null;
  }

  @Override
  V dupFloor(K key, ByteBuffer keyBuf, V value) {
    return withinRange(key) ? map.dupFloor(key, keyBuf, value) : null;
  }

  @Override
  V dupHigher(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return withinRange(key) ? map.dupHigher(key, keyBuf, valueBuf) : null;
  }

  @Override
  V dupHigher(K key, ByteBuffer keyBuf, V value) {
    return withinRange(key) ? map.dupHigher(key, keyBuf, value) : null;
  }

  @Override
  V dupLower(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return withinRange(key) ? map.dupLower(key, keyBuf, valueBuf) : null;
  }

  @Override
  V dupLower(K key, ByteBuffer keyBuf, V value) {
    return withinRange(key) ? map.dupLower(key, keyBuf, value) : null;
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
    if (null == fromKey) return map.firstEntry();
    final Map.Entry<K, V> entry = fromInclusive ? map.ceilingEntry(fromKeyBuf) : map.higherEntry(fromKeyBuf);
    if (null != entry && !withinRange(entry.getKey())) return null;
    return entry;
  }

  @Override
  public K firstKey() {
    if (null == fromKey) return map.firstKey();
    final K res = fromInclusive ? map.ceilingKey(fromKeyBuf) : map.higherKey(fromKeyBuf);
    if (null != res && !withinRange(res)) return null;
    return res;
  }

  @Override
  public Map.Entry<K, V> floorEntry(K key) {
    key = adjustKey(key);

    switch (floorMode(key)) {
      case NoPossibleMatch:
        return null;
      case UseLower:
        return handleNavigableEntryResult(map.lowerEntry(key));
      case UseFloor:
        return handleNavigableEntryResult(map.floorEntry(key));
      default:
        throw new IllegalStateException("Unknown Enum Value");
    }
  }

  @Override
  public K floorKey(K key) {
    key = adjustKey(key);

    switch (floorMode(key)) {
      case NoPossibleMatch:
        return null;
      case UseLower:
        return handleNavigableKeyResult(map.lowerKey(key));
      case UseFloor:
        return handleNavigableKeyResult(map.floorKey(key));
      default:
        throw new IllegalStateException("Unknown Enum Value");
    }
  }

  private FloorMode floorMode(K key) {
    if (!fromInclusive && Objects.equals(key, fromKey)) return FloorMode.NoPossibleMatch;
    if (!toInclusive && Objects.equals(key, toKey)) return FloorMode.UseLower;
    return FloorMode.UseFloor;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V get(Object key) {
    if (!withinRange((K) key)) return null;
    return map.get(key);
  }

  private Map.Entry<K, V> handleNavigableEntryResult(Map.Entry<K, V> entry) {
    return null != entry && withinRange(entry.getKey()) ? entry : null;
  }

  private K handleNavigableKeyResult(K key) {
    return null != key && withinRange(key) ? key : null;
  }

  @Override
  public LMDBMapInternal<K, V> headMap(K toKey) {
    return headMap(toKey, false);
  }

  @Override
  public LMDBMapInternal<K, V> headMap(K toKey, boolean toInclusive) {
    return subMap(fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public Map.Entry<K, V> higherEntry(K key) {
    final Map.Entry<K, V> entry = map.higherEntry(adjustKey(key));
    if (null != entry && !withinRange(entry.getKey())) return null;
    return entry;
  }

  @Override
  public K higherKey(K key) {
    final K res = map.higherKey(adjustKey(key));
    if (null != res && !withinRange(res)) return null;
    return res;
  }

  @Override
  public boolean isEmpty() {
    try (LMDBIterator<K> it = keySet.lmdbIterator()) {
      return it.hasNext();
    }
  }

  @Override
  public long keyCount() {
    int i = 0;

    try (LMDBIterator<K> it = keySet.lmdbIterator()) {
      while (it.hasNext()) {
        it.next();
        i++;
      }
    }

    return i;
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
    if (null == toKey) return map.lastEntry();
    final Map.Entry<K, V> entry = toInclusive ? map.floorEntry(toKeyBuf) : map.lowerEntry(toKeyBuf);
    if (null != entry && !withinRange(entry.getKey())) return null;
    return entry;
  }

  @Override
  public K lastKey() {
    if (null == toKey) return map.lastKey();
    final K res = toInclusive ? map.floorKey(toKeyBuf) : map.lowerKey(toKeyBuf);
    if (null != res && !withinRange(res)) return null;
    return res;
  }

  @Override
  public Map.Entry<K, V> lowerEntry(K key) {
    final Map.Entry<K, V> entry = map.lowerEntry(adjustKey(key));
    if (null != entry && !withinRange(entry.getKey())) return null;
    return entry;
  }

  @Override
  public K lowerKey(K key) {
    final K res = map.lowerKey(adjustKey(key));
    if (null != res && !withinRange(res)) return null;
    return res;
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
    if (null == fromKey) return map.pollFirstEntry();
    try (LMDBTxnInternal txn = map.withReadWriteTxn()) {
      Map.Entry<K, V> entry = firstEntry();
      if (null != entry) remove(entry.getKey());
      return entry;
    }
  }

  @Override
  public K pollFirstKey() {
    if (null == fromKey) return map.pollFirstKey();
    try (LMDBTxnInternal txn = map.withReadWriteTxn()) {
      final K key = firstKey();
      if (null != key) remove(key);
      return key;
    }
  }

  @Override
  public Map.Entry<K, V> pollLastEntry() {
    if (null == toKey) return map.pollLastEntry();
    try (LMDBTxnInternal txn = map.withReadWriteTxn()) {
      Map.Entry<K, V> entry = lastEntry();
      if (null != entry) remove(entry.getKey());
      return entry;
    }
  }

  @Override
  public K pollLastKey() {
    if (null == toKey) return map.pollLastKey();
    try (LMDBTxnInternal txn = map.withReadWriteTxn()) {
      final K key = lastKey();
      if (null != key) remove(key);
      return key;
    }
  }

  @Override
  public boolean prepend(K key, V value) {
    rangeCheck(key);
    return map.prepend(key, value);
  }

  @Override
  public V put(K key, V value) {
    rangeCheck(key);
    return map.put(key, value);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    try (LMDBTxnInternal txn = map.withReadWriteTxn()) {
      for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
        rangeCheck(entry.getKey());
        map.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    rangeCheck(key);
    return map.putIfAbsent(key, value);
  }

  @Override
  public void putNoPrev(K key, V value) {
    rangeCheck(key);
    map.putNoPrev(key, value);
  }

  /** Check if a key is within the valid range for this view */
  protected void rangeCheck(K key) {
    rangeCheck(key, true);
  }

  protected void rangeCheck(K key, boolean inclusive) {
    rangeCheck(key, null, inclusive);
  }

  protected void rangeCheck(K key, ByteBuffer keyBuf) {
    rangeCheck(key, keyBuf, true);
  }

  protected void rangeCheck(K key, ByteBuffer keyBuf, boolean inclusive) {
    if (!withinRange(key, keyBuf, inclusive)) throw new LMDBOutOfRangeException("Key out of range: " + key + "  fromKey: " + fromKey + " fromInclusive: " + fromInclusive + "  toKey: " + toKey + "  toInclusive: " + toInclusive);
  }

  @Override
  boolean remove(K key, ByteBuffer keyBuf, V value) {
    return withinRange(key, keyBuf) && map.remove(key, keyBuf, value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V remove(Object key) {
    rangeCheck((K) key);
    return map.remove(key);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object key, Object value) {
    rangeCheck((K) key);
    return map.remove(key, value);
  }

  @Override
  public boolean removeNoPrev(K key) {
    rangeCheck(key);
    return map.removeNoPrev(key);
  }

  @Override
  boolean removeNoPrev(K key, ByteBuffer keyBuf) {
    rangeCheck(key, keyBuf);
    return map.removeNoPrev(key, keyBuf);
  }

  @Override
  public V replace(K key, V value) {
    rangeCheck(key);
    return map.replace(key, value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    rangeCheck(key);
    return map.replace(key, oldValue, newValue);
  }

  @Override
  public int size() {
    return (int) valueCount();
  }

  @Override
  public LMDBMapInternal<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    if (null != fromKey && null != toKey && compare(fromKey, toKey) >= 0) throw new IllegalArgumentException("Expected fromKey to be less than toKey");
    return new LMDBMapView<K, V>(map, adjustKey(fromKey), fromInclusive && (null == fromKey || this.fromInclusive), adjustKey(toKey), toInclusive && (null == toKey || this.toInclusive));
  }

  @Override
  public LMDBMapInternal<K, V> subMap(K fromKey, K toKey) {
    return subMap(fromKey, true, toKey, false);
  }

  @Override
  public LMDBMapInternal<K, V> tailMap(K fromKey) {
    return tailMap(fromKey, true);
  }

  @Override
  public LMDBMapInternal<K, V> tailMap(K fromKey, boolean fromInclusive) {
    return subMap(fromKey, fromInclusive, toKey, toInclusive);
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
    int i = 0;

    try (LMDBIterator<V> it = values.lmdbIterator()) {
      while (it.hasNext()) {
        it.next();
        i++;
      }
    }

    return i;
  }

  @Override
  public LMDBValuesCollection<V> values() {
    return values;
  }

  @Override
  LMDBSerializer<V> valueSerializer() {
    return map.valueSerializer();
  }

  protected boolean withinRange(K key) {
    return withinRange(key, true);
  }

  protected boolean withinRange(K key, boolean inclusive) {
    return withinRange(key, null, inclusive);
  }

  protected boolean withinRange(K key, ByteBuffer keyBuf) {
    return withinRange(key, keyBuf, true);
  }

  protected boolean withinRange(K key, ByteBuffer keyBuf, boolean inclusive) {
    if (null == key) return false;

    boolean isGood = true;

    if (null != fromKey) {
      final int ret = map.compare(fromKey, fromKeyBuf, key, keyBuf);
      isGood = isGood && (ret < 0 || (0 == ret && (fromInclusive || !inclusive)));
    }

    if (null != toKey && isGood) {
      final int ret = map.compare(key, keyBuf, toKey, toKeyBuf);
      isGood = isGood && (ret < 0 || (0 == ret && (toInclusive || !inclusive)));
    }

    return isGood;
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
