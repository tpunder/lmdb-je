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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

class LMDBMultiSetReversed<V> extends LMDBMultiSetInternal<V> {

  private final LMDBMultiSetInternal<V> set;

  LMDBMultiSetReversed(LMDBMultiSetInternal<V> set) {
    this.set = set;
  }

  @Override
  public boolean add(V e) {
    return set.add(e);
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    return set.addAll(c);
  }

  @Override
  V ceiling(ByteBuffer buf) {
    return floor(buf);
  }

  @Override
  public V ceiling(V e) {
    return set.floor(e);
  }

  @Override
  public void clear() {
    set.clear();
  }
  
  @Override
  public void close() {
    // Nothing to close
  }

  @Override
  public Comparator<? super V> comparator() {
    return this;
  }

  @Override
  public int compare(V a, ByteBuffer aBuf, V b, ByteBuffer bBuf) {
    return -1 * set.compare(a, aBuf, b, bBuf);
  }

  @Override
  public int compare(V a, V b) {
    return -1 * set.compare(a, b);
  }

  @Override
  public boolean contains(Object o) {
    return set.contains(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return set.containsAll(c);
  }

  @Override
  public Iterator<V> descendingIterator() {
    return set.iterator();
  }
  
  @Override
  public LMDBIterator<V> descendingLMDBIterator() {
    return set.lmdbIterator();
  }

  @Override
  public LMDBMultiSet<V> descendingSet() {
    return set;
  }

  @Override
  LMDBEnvInternal env() {
    return set.env();
  }

  @Override
  public V first() {
    return set.last();
  }

  @Override
  V floor(ByteBuffer buf) {
    return ceiling(buf);
  }

  @Override
  public V floor(V e) {
    return set.ceiling(e);
  }

  @Override
  public LMDBMultiSet<V> headSet(V toElement) {
    return set.tailSet(toElement);
  }

  @Override
  public LMDBMultiSet<V> headSet(V toElement, boolean inclusive) {
    return set.tailSet(toElement, inclusive);
  }

  @Override
  V higher(ByteBuffer buf) {
    return lower(buf);
  }

  @Override
  public V higher(V e) {
    return set.lower(e);
  }

  @Override
  public boolean isEmpty() {
    return set.isEmpty();
  }
  
  @Override
  public Iterator<V> iterator() {
    return set.descendingIterator();
  }

  @Override
  public V last() {
    return set.first();
  }

  @Override
  public LMDBIterator<V> lmdbIterator() {
    return set.descendingLMDBIterator();
  }

  @Override
  V lower(ByteBuffer buf) {
    return higher(buf);
  }

  @Override
  public V lower(V e) {
    return set.higher(e);
  }

  @Override
  public V pollFirst() {
    return set.pollLast();
  }

  @Override
  public V pollLast() {
    return set.pollFirst();
  }

  @Override
  public boolean remove(Object o) {
    return set.remove(o);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return set.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return set.retainAll(c);
  }

  @Override
  public int size() {
    return set.size();
  }

  @Override
  public LMDBMultiSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive) {
    return set.subSet(toElement, toInclusive, fromElement, fromInclusive).descendingSet();
  }

  @Override
  public LMDBMultiSet<V> subSet(V fromElement, V toElement) {
    return set.subSet(toElement, false, fromElement, true).descendingSet();
  }

  @Override
  public LMDBMultiSet<V> tailSet(V fromElement) {
    return set.headSet(fromElement).descendingSet();
  }

  @Override
  public LMDBMultiSet<V> tailSet(V fromElement, boolean inclusive) {
    return set.headSet(fromElement, inclusive).descendingSet();
  }

  @Override
  LMDBSerializer<V> valueSerializer() {
    return set.valueSerializer();
  }
}
