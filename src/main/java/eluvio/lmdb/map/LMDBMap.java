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

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

public interface LMDBMap<K, V> extends LMDBEnv, ConcurrentNavigableMap<K, V>, Comparator<K>, AutoCloseable {

  /**
   * Add the key/value pair to the database if a value for the key doesn't
   * already exist.
   * <p>
   * If this is a {@link eluvio.lmdb.api.Api#MDB_DUPSORT} database (which is
   * only used internally) then the key/value pair is only added if they both
   * don't already exist in the database. This is used for implementing the
   * {@link LMDBMultiMap} and {@link LMDBMultiSet} classes.
   * 
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return true if the key/value pair was added or false if the key (or
   *         key/data pair if using dups) already exists
   */
  boolean add(K key, V value);

  /**
   * A possibly optimized {@link #putNoPrev} operation when you know they key is
   * greater than all other keys in the map.
   * <p>
   * If this operation is not optimized then it will simply be a call to
   * {@link #putNoPrev}
   * 
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return true if the append was successful (i.e. the key did not exist and was greater than all other keys)
   */
  boolean append(K key, V value);

  @Override
  void close();

  boolean contains(K key, V value);

  @Override
  LMDBKeySet<K> descendingKeySet();

  @Override
  LMDBMap<K, V> descendingMap();

  @Override
  LMDBSet<Map.Entry<K, V>> entrySet();

  @Override
  LMDBMap<K, V> headMap(K toKey);

  @Override
  LMDBMap<K, V> headMap(K toKey, boolean toInclusive);

  /**
   * The number of unique keys in the database
   * 
   * @return number of unique keys in the database
   */
  long keyCount();

  @Override
  LMDBKeySet<K> keySet();

  @Override
  LMDBKeySet<K> navigableKeySet();

  /**
   * Retrieves and removes the first (lowest) key, or returns null if this map
   * is empty.
   * 
   * @return the first key, or null if the map is empty
   * @see LMDBKeySet#pollFirst
   */
  K pollFirstKey();

  /**
   * Retrieves and removes the first (lowest) key's value, or returns null if this map
   * is empty.
   *
   * @return the first value, or null if the map is empty
   */
  V pollFirstValue();

  /**
   * Retrieves and removes the last (highest) key, or returns null if this map
   * is empty.
   * 
   * @return the last key, or null if the map is empty
   * @see LMDBKeySet#pollLast
   */
  K pollLastKey();

  /**
   * Retrieves and removes the last (highest) key's value, or returns null if this map
   * is empty.
   *
   * @return the last key, or null if the map is empty
   * @see LMDBKeySet#pollLast
   */
  V pollLastValue();

  /**
   * A possibly optimized {@link #putNoPrev} operation when you know they key is
   * less than than all other keys in the map.
   * <p>
   * If this operation is not optimized then it will simply be a call to
   * {@link #putNoPrev}
   * 
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return true if the prepend was successful (i.e. the key did not exist and was less than all other keys
   */
  boolean prepend(K key, V value);

  /**
   * Same as the normal {@link #put} method but does not return the previous
   * value.
   * 
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @see #put
   */
  void putNoPrev(K key, V value);

  /**
   * Same as the normal {@link #remove} method but does not return the previous
   * value.
   * 
   * @param key key to remove
   * @see #remove
   */
  boolean removeNoPrev(K key);

  @Override
  LMDBMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

  @Override
  LMDBMap<K, V> subMap(K fromKey, K toKey);

  @Override
  LMDBMap<K, V> tailMap(K fromKey);

  @Override
  LMDBMap<K, V> tailMap(K fromKey, boolean fromInclusive);

  /**
   * The number of values in the database
   * 
   * @return number of values in the database
   */
  long valueCount();

  @Override
  LMDBCollection<V> values();
}
