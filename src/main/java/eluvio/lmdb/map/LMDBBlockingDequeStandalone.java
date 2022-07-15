/*
 * Copyright 2021 Tim Underwood (https://github.com/tpunder)
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

import java.io.Closeable;
import java.io.File;

/**
 * An {@link LMDBBlockingDeque} implementation that is a self-contained LMDB environment
 * with a single database in it used for the queue. This uses a {@link LMDBMapStandalone}
 * instance internally.
 * @param <E> queue element type
 */
public class LMDBBlockingDequeStandalone<E> extends LMDBBlockingDeque<E> implements LMDBEnv, Closeable {
  final LMDBMapStandalone<Long,E> map;

  public LMDBBlockingDequeStandalone(LMDBSerializer<E> serializer) {
    this(null, serializer, LMDBBlockingDeque.UNBOUNDED);
  }

  public LMDBBlockingDequeStandalone(LMDBSerializer<E> serializer, int capacity) {
    this(null, serializer, capacity);
  }

  public LMDBBlockingDequeStandalone(File path, LMDBSerializer<E> serializer) {
    this(path, serializer, LMDBBlockingDeque.UNBOUNDED);
  }

  public LMDBBlockingDequeStandalone(File path, LMDBSerializer<E> serializer, int capacity) {
    this(path, serializer, capacity, DEFAULT_MAPSIZE);
  }

  public LMDBBlockingDequeStandalone(File path, LMDBSerializer<E> serializer, int capacity, long mapsize) {
    this(new LMDBMapStandalone<Long, E>(path, LMDBSerializer.Long, serializer, false, mapsize), capacity);
  }

  private LMDBBlockingDequeStandalone(LMDBMapStandalone<Long,E> map, int capacity) {
    super(map, capacity);
    this.map = map;
  }

  @Override
  public void abortTxn() { map.abortTxn(); }

  @Override
  public void beginTxn() { map.beginTxn(); }

  @Override
  public void beginTxn(boolean readOnly) { map.beginTxn(readOnly); }

  @Override
  public void close() { map.close(); }

  @Override
  public void commitTxn() { map.commitTxn(); }

  @Override
  final public ReusableTxn detachTxnFromCurrentThread() {
    return map.detachTxnFromCurrentThread();
  }

  @Override
  public boolean readOnly() { return map.readOnly(); }

  @Override
  public LMDBTxn withExistingReadOnlyTxn() { return map.withExistingReadOnlyTxn(); }

  @Override
  public LMDBTxn withExistingReadWriteTxn() { return map.withExistingReadWriteTxn(); }

  @Override
  public LMDBTxn withExistingTxn() { return map.withExistingTxn(); }

  @Override
  public LMDBTxn withNestedReadWriteTxn() { return map.withNestedReadWriteTxn(); }

  @Override
  public LMDBTxn withReadOnlyTxn() { return map.withReadOnlyTxn(); }

  @Override
  public LMDBTxn withReadWriteTxn() { return map.withReadWriteTxn(); }

  @Override
  public void disableMetaSync() { map.disableMetaSync(); }

  @Override
  public void enableMetaSync() { map.enableMetaSync(); }

  @Override
  public void disableSync() { map.disableSync(); }

  @Override
  public void enableSync() { map.enableSync(); }

  @Override
  public void sync() { map.sync(); }

  @Override
  public void sync(boolean force) { map.sync(force); }
}
