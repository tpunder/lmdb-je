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
