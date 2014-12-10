package eluvio.lmdb.map;

import java.util.Comparator;
import java.util.Map.Entry;

class LMDBMultiMapImpl<K, V> implements LMDBMultiMap<K, V> {
  final LMDBMapInternal<K, V> map;

  public LMDBMultiMapImpl(LMDBEnvInternal env, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, Comparator<V> valueComparator) {
    this(new LMDBMapImpl<K, V>(env, keySerializer, valueSerializer, keyComparator, valueComparator, true));
  }

  public LMDBMultiMapImpl(LMDBMapInternal<K, V> map) {
    this.map = map;
  }

  @Override
  public boolean add(K key, V value) {
    return map.add(key, value);
  }

  @Override
  public K ceilingKey(K key) {
    return map.ceilingKey(key);
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
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(V value) {
    return map.containsValue(value);
  }

  @Override
  public LMDBKeySet<K> descendingKeySet() {
    return map.descendingKeySet();
  }

  @Override
  public LMDBMultiMap<K, V> descendingMap() {
    return new LMDBMultiMapImpl<K, V>(map.descendingMap());
  }

  @Override
  public LMDBSet<Entry<K, V>> entrySet() {
    return map.entrySet();
  }

  @Override
  public K firstKey() {
    return map.firstKey();
  }

  @Override
  public K floorKey(K key) {
    return map.floorKey(key);
  }

  @Override
  public LMDBMultiSet<V> get(K key) {
    return new LMDBMultiSetImpl<K, V>(map, key);
  }

  @Override
  public LMDBMultiMap<K, V> headMap(K toKey) {
    return new LMDBMultiMapImpl<K, V>(map.headMap(toKey));
  }

  @Override
  public LMDBMultiMap<K, V> headMap(K toKey, boolean toInclusive) {
    return new LMDBMultiMapImpl<K, V>(map.headMap(toKey, toInclusive));
  }

  @Override
  public K higherKey(K key) {
    return map.higherKey(key);
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
  public LMDBKeySet<K> keySet() {
    return map.keySet();
  }

  @Override
  public K lastKey() {
    return map.lastKey();
  }

  @Override
  public K lowerKey(K key) {
    return map.lowerKey(key);
  }

  @Override
  public LMDBKeySet<K> navigableKeySet() {
    return map.navigableKeySet();
  }

  @Override
  public boolean remove(K key, V value) {
    return map.remove(key, value);
  }

  @Override
  public boolean removeAll(K key) {
    return map.removeNoPrev(key);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public LMDBMultiMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return new LMDBMultiMapImpl<K, V>(map.subMap(fromKey, fromInclusive, toKey, toInclusive));
  }

  @Override
  public LMDBMultiMap<K, V> subMap(K fromKey, K toKey) {
    return new LMDBMultiMapImpl<K, V>(map.subMap(fromKey, toKey));
  }

  @Override
  public LMDBMultiMap<K, V> tailMap(K fromKey) {
    return new LMDBMultiMapImpl<K, V>(map.tailMap(fromKey));
  }

  @Override
  public LMDBMultiMap<K, V> tailMap(K fromKey, boolean fromInclusive) {
    return new LMDBMultiMapImpl<K, V>(map.tailMap(fromKey, fromInclusive));
  }

  @Override
  public long valueCount() {
    return map.valueCount();
  }

  @Override
  public LMDBCollection<V> values() {
    return map.values();
  }

}
