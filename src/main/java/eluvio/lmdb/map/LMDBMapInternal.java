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

abstract class LMDBMapInternal<K, V> implements LMDBMap<K, V> {
  @Override
  final public void abortTxn() {
    env().abortTxn();
  }

  abstract boolean add(K key, ByteBuffer keyBuf, V value);

  @Override
  final public void beginTxn() {
    env().beginTxn();
  }

  @Override
  final public void beginTxn(boolean readOnly) {
    env().beginTxn(readOnly);
  }

  @Override
  final public void commitTxn() {
    env().commitTxn();
  }

  @Override
  final public ReusableTxn detatchTxnFromCurrentThread() {
    return env().detatchTxnFromCurrentThread();
  }

  /**
   * An optimized version of {@link #compare(Object,Object)} that can take
   * already serialized {@link ByteBuffer}'s to prevent duplicate serialization
   * 
   * @param a the first object to be compared
   * @param aBuf a {@link ByteBuffer} representing the first object or null
   * @param b the second object to be compared
   * @param bBuf a {@link ByteBuffer} representing the second object or null
   * @return a negative integer, zero, or a positive integer as the first
   *         argument is less than, equal to, or greater than the second
   * @see #compare(Object,Object)
   */
  abstract int compare(K a, ByteBuffer aBuf, K b, ByteBuffer bBuf);

  abstract boolean contains(K key, ByteBuffer keyBuf, V value);

  public abstract LMDBMapInternal<K, V> descendingMap();

  abstract boolean dup();

  abstract V dupCeiling(K key, ByteBuffer keyBuf, ByteBuffer valueBuf);

  abstract V dupCeiling(K key, ByteBuffer keyBuf, V value);

  abstract V dupFloor(K key, ByteBuffer keyBuf, ByteBuffer valueBuf);

  abstract V dupFloor(K key, ByteBuffer keyBuf, V value);

  abstract V dupHigher(K key, ByteBuffer keyBuf, ByteBuffer valueBuf);

  abstract V dupHigher(K key, ByteBuffer keyBuf, V value);

  abstract V dupLower(K key, ByteBuffer keyBuf, ByteBuffer valueBuf);

  abstract V dupLower(K key, ByteBuffer keyBuf, V value);

  abstract LMDBEnvInternal env();

  public abstract LMDBMapInternal<K, V> headMap(K toKey);

  public abstract LMDBMapInternal<K, V> headMap(K toKey, boolean toInclusive);

  abstract LMDBSerializer<K> keySerializer();

  final LMDBCursor<K, V> openReadOnlyCursor() {
    return openCursor(LMDBCursor.Mode.READ_ONLY);
  }
  
  final LMDBCursor<K, V> openReadWriteCursor() {
    return openCursor(LMDBCursor.Mode.READ_WRITE);
  }
  
  final LMDBCursor<K, V> openCursorExistingTxn() {
    return openCursor(LMDBCursor.Mode.USE_EXISTING_TXN);
  }

  abstract LMDBCursor<K, V> openCursor(LMDBCursor.Mode mode);

  @Override
  final public boolean readOnly() {
    return env().readOnly();
  }

  abstract boolean remove(K key, ByteBuffer keyBuf, V value);

  abstract boolean removeNoPrev(K key, ByteBuffer keyBuf);

  public abstract LMDBMapInternal<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

  public abstract LMDBMapInternal<K, V> subMap(K fromKey, K toKey);

  public abstract LMDBMapInternal<K, V> tailMap(K fromKey);

  public abstract LMDBMapInternal<K, V> tailMap(K fromKey, boolean fromInclusive);

  abstract Comparator<V> valueComparator();

  abstract int valueCompare(V a, ByteBuffer aBuf, V b, ByteBuffer bBuf);

  abstract int valueCompare(V a, V b);

  abstract LMDBSerializer<V> valueSerializer();

  @Override
  final public LMDBTxnInternal withExistingReadOnlyTxn() {
    return env().withExistingReadOnlyTxn();
  }

  @Override
  final public LMDBTxnInternal withExistingReadWriteTxn() {
    return env().withExistingReadWriteTxn();
  }
  
  @Override
  final public LMDBTxnInternal withExistingTxn() {
    return env().withExistingTxn();
  }

  @Override
  final public LMDBTxnInternal withNestedReadWriteTxn() {
    return env().withNestedReadWriteTxn();
  }

  @Override
  final public LMDBTxnInternal withReadOnlyTxn() {
    return env().withReadOnlyTxn();
  }

  @Override
  final public LMDBTxnInternal withReadWriteTxn() {
    return env().withReadWriteTxn();
  }
}
