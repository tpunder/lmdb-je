package eluvio.lmdb.map;

import java.util.Map;

/**
 * An LMDB Map with multiple values per key
 * <p>
 * This is backed by an LMDB database with the
 * {@link eluvio.lmdb.api.Api#MDB_DUPSORT} flag set
 * 
 * @param <K> key type
 * @param <V> value type
 */
public interface LMDBMultiMap<K, V> extends LMDBEnv, AutoCloseable {
  boolean add(K key, V value);

  K ceilingKey(K key);

  void clear();

  void close();

  boolean containsKey(K key);

  boolean containsValue(V value);

  LMDBKeySet<K> descendingKeySet();

  LMDBMultiMap<K, V> descendingMap();

  LMDBSet<Map.Entry<K, V>> entrySet();

  K firstKey();

  K floorKey(K key);

  /**
   * Returns the set of values associated with the key.
   * <p>
   * Changes made to the set are reflected in the underlying LMDB database.
   * 
   * @param key The key to lookup values for
   * @return the set of values associated with the key
   */
  LMDBMultiSet<V> get(K key);

  LMDBMultiMap<K, V> headMap(K toKey);

  LMDBMultiMap<K, V> headMap(K toKey, boolean toInclusive);

  K higherKey(K key);

  boolean isEmpty();

  long keyCount();

  LMDBKeySet<K> keySet();

  K lastKey();

  K lowerKey(K key);

  LMDBKeySet<K> navigableKeySet();

  boolean remove(K key, V value);

  boolean removeAll(K key);

  int size();

  LMDBMultiMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

  LMDBMultiMap<K, V> subMap(K fromKey, K toKey);

  LMDBMultiMap<K, V> tailMap(K fromKey);

  LMDBMultiMap<K, V> tailMap(K fromKey, boolean fromInclusive);

  long valueCount();

  LMDBCollection<V> values();
}
