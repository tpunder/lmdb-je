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

import java.io.File;
import java.util.Comparator;

public class LMDBMultiMapStandalone<K, V> extends LMDBMultiMapImpl<K, V> implements LMDBEnv {
  public LMDBMultiMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer) {
    this(path, keySerializer, valueSerializer, null, false, DEFAULT_MAPSIZE);
  }

  public LMDBMultiMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, boolean readOnly) {
    this(path, keySerializer, valueSerializer, null, readOnly, DEFAULT_MAPSIZE);
  }

  public LMDBMultiMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, boolean readOnly, long mapsize) {
    this(path, keySerializer, valueSerializer, null, readOnly, mapsize);
  }

  public LMDBMultiMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator) {
    this(path, keySerializer, valueSerializer, keyComparator, false, DEFAULT_MAPSIZE);
  }

  public LMDBMultiMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, boolean readOnly, long mapsize) {
    this(path, keySerializer, valueSerializer, keyComparator, null, readOnly, mapsize);
  }

  public LMDBMultiMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, Comparator<V> valueComparator, boolean readOnly, long mapsize) {
    super(new LMDBEnvImpl(path, readOnly, mapsize), keySerializer, valueSerializer, keyComparator, valueComparator);
  }

  public LMDBMultiMapStandalone(LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer) {
    this(null, keySerializer, valueSerializer);
  }

  public LMDBMultiMapStandalone(LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator) {
    this(null, keySerializer, valueSerializer, keyComparator);
  }

  public LMDBMultiMapStandalone(LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, long mapsize) {
    this(null, keySerializer, valueSerializer, keyComparator, false, mapsize);
  }

  @Override
  public void abortTxn() {
    map.env().abortTxn();
  }

  @Override
  public void beginTxn() {
    map.env().beginTxn();
  }

  @Override
  public void beginTxn(boolean readOnly) {
    map.env().beginTxn(readOnly);
  }

  @Override
  public void close() {
    map.env().closeTransactions();
    super.close();
    map.env().close();
  }

  @Override
  public void commitTxn() {
    map.env().commitTxn();
  }

  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }

  @Override
  public boolean readOnly() {
    return map.env().readOnly();
  }

  @Override
  public LMDBTxn withExistingReadOnlyTxn() {
    return map.withExistingReadOnlyTxn();
  }
  
  @Override
  public LMDBTxn withExistingReadWriteTxn() {
    return map.withExistingReadWriteTxn();
  }

  @Override
  public LMDBTxn withExistingTxn() {
    return map.withExistingTxn();
  }

  @Override
  public LMDBTxn withNestedReadWriteTxn() {
    return map.withNestedReadWriteTxn();
  }

  public LMDBTxnInternal withReadOnlyTxn() {
    return map.withReadOnlyTxn();
  }
  
  public LMDBTxnInternal withReadWriteTxn() {
    return map.withReadWriteTxn();
  }
}
