package eluvio.lmdb.map;

import java.util.Collection;
import java.util.Iterator;

final class LMDBValuesCollection<V> implements LMDBCollection<V> {
  private final LMDBMapInternal<?,V> map;
  
  LMDBValuesCollection(LMDBMapInternal<?,V> map) {
    this.map = map;
  }
  
  @Override
  public boolean add(V e) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean addAll(Collection<? extends V> c) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(Object o) {
    return map.containsValue(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    try (LMDBTxn txn = map.withReadOnlyTxn()) {
      for (Object e : c) {
        if (!contains(e)) return false;
      }
        
      return true;
    }
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public Iterator<V> iterator() {
    return LMDBIteratorImpl.forValues(map, LMDBCursor.Mode.USE_EXISTING_TXN);
  }

  @Override
  public LMDBIterator<V> lmdbIterator() {
    return LMDBIteratorImpl.forValues(map, LMDBCursor.Mode.READ_ONLY);
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return map.size();
  }
  
  @Override
  public Object[] toArray() {
    try (LMDBTxnInternal txn = map.withReadOnlyTxn()) {
      
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
  public <T> T[] toArray(T[] a) {
    try (LMDBTxnInternal txn = map.withReadOnlyTxn()) {
      final int size = size();
      T[] r = a.length >= size ? a : (T[])java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
      try (LMDBIterator<V> it = lmdbIterator()) {
        for (int i = 0; i < r.length; i++) {
          if (!it.hasNext()) r[i] = null; // null terminate
          else r[i] = (T)it.next();
        }
      }
      return r;
    }
  }
}
