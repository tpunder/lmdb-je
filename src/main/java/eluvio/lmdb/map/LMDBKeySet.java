package eluvio.lmdb.map;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;

public class LMDBKeySet<K> implements NavigableSet<K>, LMDBSet<K> {
  private final LMDBMapInternal<K,?> map;
  
  LMDBKeySet(LMDBMapInternal<K,?> map) {
    this.map = map;
  }

  @Override
  public boolean add(K key) {
    throw new UnsupportedOperationException("Cannot add to this NavigableSet");
  }
  
  @Override
  public boolean addAll(Collection<? extends K> c) {
    throw new UnsupportedOperationException("Cannot addAll to this NavigableSet");
  }
  
  @Override
  public K ceiling(K key) {
    return map.ceilingKey(key);
  }
  
  @Override
  public void clear() {
    map.clear();
  }
  
  @Override
  public Comparator<? super K> comparator() {
    return map.comparator();
  }
  
  @Override
  public boolean contains(Object key) {
    return map.containsKey(key);
  }
  
  @Override
  public boolean containsAll(Collection<?> c) {
    try (LMDBTxnInternal txn = map.withReadOnlyTxn()) {
      for (Object v : c) {
        if (!contains(v)) return false;
      }
    }
    
    return true;
  }
  
  @Override
  public Iterator<K> descendingIterator() {
    return map.descendingMap().keySet().iterator();
  }
  
  public LMDBIterator<K> descendingLMDBIterator() {
    return map.descendingMap().keySet().lmdbIterator();
  }
  
  @Override
  public LMDBKeySet<K> descendingSet() {
    return map.descendingMap().keySet();
  }
  
  @Override
  public K first() {
    return map.firstKey();
  }
  
  @Override
  public K floor(K key) {
    return map.floorKey(key);
  }
  
  @Override
  public NavigableSet<K> headSet(K toKey) {
    return headSet(toKey, false);
  }
  
  @Override
  public NavigableSet<K> headSet(K toKey, boolean toInclusive) {
    return map.headMap(toKey, toInclusive).keySet();
  }
  
  @Override
  public K higher(K key) {
    return map.higherKey(key);
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  @Override
  public Iterator<K> iterator() {
    return LMDBIteratorImpl.forKeys(map, LMDBCursor.Mode.USE_EXISTING_TXN);
  }
  
  @Override
  public K last() {
    return map.lastKey();
  }

  @Override
  public LMDBIterator<K> lmdbIterator() {
    return LMDBIteratorImpl.forKeys(map, LMDBCursor.Mode.READ_ONLY);
  }
  
  @Override
  public K lower(K key) {
    return map.lowerKey(key);
  }
  
  @Override
  public K pollFirst() {
    return map.pollFirstKey();
  }
  
  @Override
  public K pollLast() {
    return map.pollLastKey();
  }
  
  @Override
  public boolean remove(Object key) {
    return null != map.remove(key);
  }
  
  @Override
  public boolean removeAll(Collection<?> c) {
    boolean changed = false;
    
    try (LMDBTxnInternal txn = map.withReadWriteTxn()){
      for (Object key : c) {
        if (remove(key)) changed = true;
      }
    }
    
    return changed;
  }
  
  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not Supported");
  }
  
  @Override
  public int size() {
    return (int)map.keyCount();
  }
  
  public long sizeLong() {
    return map.keyCount();
  }
  
  @Override
  public NavigableSet<K> subSet(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return map.subMap(fromKey, fromInclusive, toKey, toInclusive).keySet();
  }
  
  @Override
  public NavigableSet<K> subSet(K fromKey, K toKey) {
    return subSet(fromKey, true, toKey, false);
  }
  
  @Override
  public NavigableSet<K> tailSet(K fromKey) {
    return tailSet(fromKey, true);
  }
  
  @Override
  public NavigableSet<K> tailSet(K fromKey, boolean fromInclusive) {
    return map.tailMap(fromKey, fromInclusive).keySet();
  }
  
  @Override
  public Object[] toArray() {
    try (LMDBTxnInternal txn = map.withReadOnlyTxn()) {
      
      Object[] arr = new Object[size()];
      try (LMDBIterator<K> it = lmdbIterator()) {
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
      try (LMDBIterator<K> it = lmdbIterator()) {
        for (int i = 0; i < r.length; i++) {
          if (!it.hasNext()) r[i] = null; // null terminate
          else r[i] = (T)it.next();
        }
      }
      return r;
    }
  }
}
