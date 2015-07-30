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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

class LMDBEntrySet<K,V> implements LMDBSet<Map.Entry<K,V>> {
  private final LMDBMapInternal<K,V> map;
  
  LMDBEntrySet(LMDBMapInternal<K,V> map) {
    this.map = map;
  }
  
  @Override
  public boolean add(Entry<K, V> e) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean addAll(Collection<? extends Entry<K, V>> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(Object o) {
    final Map.Entry<K,V> entry = (Map.Entry<K,V>)o;
    return map.contains(entry.getKey(), entry.getValue());
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean containsAll(Collection<?> c) {
    try(final LMDBTxn txn = map.withReadOnlyTxn()) {
      for (Object o : c) {
        final Map.Entry<K,V> entry = (Map.Entry<K,V>)o;
        if (!map.contains(entry.getKey(), entry.getValue())) return false;
      }
      
      return true;
    }
    
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  @Override
  public Iterator<Map.Entry<K,V>> iterator() {
    return LMDBIteratorImpl.forEntries(map, LMDBCursor.Mode.USE_EXISTING_TXN);
  }

  @Override
  public LMDBIterator<Map.Entry<K,V>> lmdbIterator() {
    return LMDBIteratorImpl.forEntries(map, LMDBCursor.Mode.READ_ONLY);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object o) {
    final Map.Entry<K,V> entry = (Map.Entry<K,V>)o;
    return map.remove(entry.getKey(), entry.getValue());
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
      try (LMDBIterator<Map.Entry<K,V>> it = lmdbIterator()) {
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
      try (LMDBIterator<Map.Entry<K,V>> it = lmdbIterator()) {
        for (int i = 0; i < r.length; i++) {
          if (!it.hasNext()) r[i] = null; // null terminate
          else r[i] = (T)it.next();
        }
      }
      return r;
    }
  }
}
