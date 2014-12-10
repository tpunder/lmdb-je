package eluvio.lmdb.map;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

class LMDBMapProxy<K, V> extends LMDBMapInternal<K, V> {
  protected LMDBMapInternal<K, V> self;

  public LMDBMapProxy() {
    this(null);
  }

  public LMDBMapProxy(LMDBMapInternal<K, V> self) {
    this.self = self;
  }

  @Override
  boolean add(K key, ByteBuffer keyBuf, V value) {
    return self.add(key, keyBuf, value);
  }

  @Override
  public boolean add(K key, V value) {
    return self.add(key, value);
  }

  @Override
  public boolean append(K key, V value) {
    return self.append(key, value);
  }

  @Override
  public java.util.Map.Entry<K, V> ceilingEntry(K key) {
    return self.ceilingEntry(key);
  }

  @Override
  public K ceilingKey(K key) {
    return self.ceilingKey(key);
  }

  @Override
  public void clear() {
    self.clear();
  }

  @Override
  public void close() {
    self.close();
  }

  @Override
  public Comparator<? super K> comparator() {
    return self.comparator();
  }

  @Override
  public int compare(K a, ByteBuffer aBuf, K b, ByteBuffer bBuf) {
    return self.compare(a, aBuf, b, bBuf);
  }

  @Override
  public int compare(K a, K b) {
    return self.compare(a, b);
  }

  @Override
  public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return self.compute(key, remappingFunction);
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return self.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return self.computeIfPresent(key, remappingFunction);
  }

  @Override
  boolean contains(K key, ByteBuffer keyBuf, V value) {
    return self.contains(key, keyBuf, value);
  }

  @Override
  public boolean contains(K key, V value) {
    return self.contains(key, value);
  }

  @Override
  public boolean containsKey(Object key) {
    return self.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return self.containsValue(value);
  }

  @Override
  public LMDBKeySet<K> descendingKeySet() {
    return self.descendingKeySet();
  }

  @Override
  public LMDBMapInternal<K, V> descendingMap() {
    return self.descendingMap();
  }

  @Override
  boolean dup() {
    return self.dup();
  }

  @Override
  V dupCeiling(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return self.dupCeiling(key, keyBuf, valueBuf);
  }

  @Override
  V dupCeiling(K key, ByteBuffer keyBuf, V value) {
    return self.dupCeiling(key, keyBuf, value);
  }

  @Override
  V dupFloor(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return self.dupFloor(key, keyBuf, valueBuf);
  }

  @Override
  V dupFloor(K key, ByteBuffer keyBuf, V value) {
    return self.dupFloor(key, keyBuf, value);
  }

  @Override
  V dupHigher(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return self.dupHigher(key, keyBuf, valueBuf);
  }

  @Override
  V dupHigher(K key, ByteBuffer keyBuf, V value) {
    return self.dupHigher(key, keyBuf, value);
  }

  @Override
  V dupLower(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    return self.dupLower(key, keyBuf, valueBuf);
  }

  @Override
  V dupLower(K key, ByteBuffer keyBuf, V value) {
    return self.dupLower(key, keyBuf, value);
  }

  @Override
  public LMDBSet<Map.Entry<K, V>> entrySet() {
    return self.entrySet();
  }

  @Override
  LMDBEnvInternal env() {
    return self.env();
  }

  @Override
  public boolean equals(Object o) {
    return self.equals(o);
  }

  @Override
  public java.util.Map.Entry<K, V> firstEntry() {
    return self.firstEntry();
  }

  @Override
  public K firstKey() {
    return self.firstKey();
  }

  @Override
  public Map.Entry<K, V> floorEntry(K key) {
    return self.floorEntry(key);
  }

  @Override
  public K floorKey(K key) {
    return self.floorKey(key);
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    self.forEach(action);
  }

  @Override
  public V get(Object key) {
    return self.get(key);
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    return self.getOrDefault(key, defaultValue);
  }

  @Override
  public int hashCode() {
    return self.hashCode();
  }

  @Override
  public LMDBMapInternal<K, V> headMap(K toKey) {
    return self.headMap(toKey);
  }

  @Override
  public LMDBMapInternal<K, V> headMap(K toKey, boolean inclusive) {
    return self.headMap(toKey, inclusive);
  }

  @Override
  public java.util.Map.Entry<K, V> higherEntry(K key) {
    return self.higherEntry(key);
  }

  @Override
  public K higherKey(K key) {
    return self.higherKey(key);
  }

  @Override
  public boolean isEmpty() {
    return self.isEmpty();
  }

  @Override
  public long keyCount() {
    return self.keyCount();
  }

  @Override
  LMDBSerializer<K> keySerializer() {
    return self.keySerializer();
  }

  @Override
  public LMDBKeySet<K> keySet() {
    return self.keySet();
  }

  @Override
  public java.util.Map.Entry<K, V> lastEntry() {
    return self.lastEntry();
  }

  @Override
  public K lastKey() {
    return self.lastKey();
  }

  @Override
  public Map.Entry<K, V> lowerEntry(K key) {
    return self.lowerEntry(key);
  }

  @Override
  public K lowerKey(K key) {
    return self.lowerKey(key);
  }

  @Override
  public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    return self.merge(key, value, remappingFunction);
  }

  @Override
  public LMDBKeySet<K> navigableKeySet() {
    return self.navigableKeySet();
  }

  @Override
  protected LMDBCursor<K, V> openCursor(LMDBCursor.Mode mode) {
    return self.openCursor(mode);
  }

  @Override
  public java.util.Map.Entry<K, V> pollFirstEntry() {
    return self.pollFirstEntry();
  }

  @Override
  public K pollFirstKey() {
    return self.pollFirstKey();
  }

  @Override
  public java.util.Map.Entry<K, V> pollLastEntry() {
    return self.pollLastEntry();
  }

  @Override
  public K pollLastKey() {
    return self.pollLastKey();
  }

  @Override
  public boolean prepend(K key, V value) {
    return self.prepend(key, value);
  }

  @Override
  public V put(K key, V value) {
    return self.put(key, value);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    self.putAll(m);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return self.putIfAbsent(key, value);
  }

  @Override
  public void putNoPrev(K key, V value) {
    self.putNoPrev(key, value);
  }

  @Override
  boolean remove(K key, ByteBuffer keyBuf, V value) {
    return self.remove(key, keyBuf, value);
  }

  @Override
  public V remove(Object key) {
    return self.remove(key);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return self.remove(key, value);
  }

  @Override
  public boolean removeNoPrev(K key) {
    return self.removeNoPrev(key);
  }

  @Override
  boolean removeNoPrev(K key, ByteBuffer keyBuf) {
    return self.removeNoPrev(key, keyBuf);
  }

  @Override
  public V replace(K key, V value) {
    return self.replace(key, value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return self.replace(key, oldValue, newValue);
  }

  @Override
  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    self.replaceAll(function);
  }

  @Override
  public int size() {
    return self.size();
  }

  @Override
  public LMDBMapInternal<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return self.subMap(fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public LMDBMapInternal<K, V> subMap(K fromKey, K toKey) {
    return self.subMap(fromKey, toKey);
  }

  @Override
  public LMDBMapInternal<K, V> tailMap(K fromKey) {
    return self.tailMap(fromKey);
  }

  @Override
  public LMDBMapInternal<K, V> tailMap(K fromKey, boolean inclusive) {
    return self.tailMap(fromKey, inclusive);
  }

  @Override
  Comparator<V> valueComparator() {
    return self.valueComparator();
  }

  @Override
  int valueCompare(V a, ByteBuffer aBuf, V b, ByteBuffer bBuf) {
    return self.valueCompare(a, aBuf, b, bBuf);
  }

  @Override
  int valueCompare(V a, V b) {
    return self.valueCompare(a, b);
  }

  @Override
  public long valueCount() {
    return self.valueCount();
  }

  @Override
  public LMDBCollection<V> values() {
    return self.values();
  }

  @Override
  LMDBSerializer<V> valueSerializer() {
    return self.valueSerializer();
  }
}
