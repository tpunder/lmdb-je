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

abstract class LMDBMultiSetInternal<V> implements LMDBMultiSet<V>, LMDBEnv {
  
  @Override
  final public void abortTxn() {
    env().abortTxn();
  }

  @Override
  final public void beginTxn() {
    env().beginTxn();
  }

  @Override
  final public void beginTxn(boolean readOnly) {
    env().beginTxn(readOnly);
  }
  
  abstract V ceiling(ByteBuffer valueBuf);
  
  @Override
  final public void commitTxn() {
    env().commitTxn();
  }

  @Override
  final public ReusableTxn detachTxnFromCurrentThread() {
    return env().detachTxnFromCurrentThread();
  }

  public abstract int compare(V a, ByteBuffer aBuf, V b, ByteBuffer bBuf);

  abstract LMDBEnvInternal env();
  
  abstract V floor(ByteBuffer valueBuf);
  
  abstract V higher(ByteBuffer valueBuf);

  abstract V lower(ByteBuffer valueBuf);

  @Override
  final public boolean readOnly() {
    return env().readOnly();
  }
  
  @Override
  final public Object[] toArray() {
    try (final LMDBTxnInternal txn = withReadOnlyTxn()) {

      Object[] arr = new Object[size()];
      try (LMDBIterator<V> it = lmdbIterator()) {
        int i = 0;
        while (it.hasNext()) {
          arr[i] = it.next();
          i++;
        }
      }

      return arr;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  final public <T> T[] toArray(T[] a) {
    try (final LMDBTxnInternal txn = withReadOnlyTxn()) {
      final int size = size();
      T[] r = a.length >= size ? a : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
      try (LMDBIterator<V> it = lmdbIterator()) {
        for (int i = 0; i < r.length; i++) {
          if (!it.hasNext()) r[i] = null; // null terminate
          else r[i] = (T) it.next();
        }
      }
      return r;
    }
  }

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
