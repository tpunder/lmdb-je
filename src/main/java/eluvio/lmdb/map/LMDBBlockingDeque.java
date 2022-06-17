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

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * An optionally bounded java.util.concurrent.BlockingDeque implementation on top of an {@link LMDBMap}.
 *
 * @param <E> The type of element you are storing in the queue
 */
public class LMDBBlockingDeque<E> implements BlockingDeque<E> {
  public static final int UNBOUNDED = -1;

  /**
   * We start at 0 for our key go negative for prepends and positive for appends. For any reasonable usage of this queue
   * that should provide enough range. Whenever the queue is empty it will reset to this key.
   *
   * For any long running usage that would potentially add more than Long.MAX_VALUE elements to this queue over time we
   * would need to come up with a strategy for handling the overflow.
   *
   * Underflow/Overflow is currently detected and an AssertionError is thrown.
   */
  static final long STARTING_KEY = 0L;

  /** The max capacity of our queue or -1 if no max capacity */
  final int capacity;

  /** The underlying LMDB Map we are using to implement this queue with. Nobody else should be modifying this map. */
  final LMDBMap<Long,E> map;

  /** This will be used for managing write capacity into the queue (can be optionally null) */
  final Semaphore writeCapacitySemaphore;

  /** This will use used for managing reads from the Queue */
  final Semaphore readCapacitySemaphore = new Semaphore(0, true);

  public LMDBBlockingDeque(LMDBMap<Long,E> map) {
    this(map, UNBOUNDED);
  }

  /**
   *
   * @param map The backing LMDBMap. This should not be modified externally
   * @param capacity The max capacity (or -1 if this Queue is unbounded)
   */
  public LMDBBlockingDeque(final LMDBMap<Long,E> map, final int capacity) {
    // Note: We would need to dynamically set STARTING_KEY if we needed to support LMDBSerializer.UnsignedLong
    if (map.compare(Long.MIN_VALUE, Long.MAX_VALUE) >= 0) throw new IllegalArgumentException("Invalid map comparator. Long.MIN_VALUE must be less than Long.MAX_VALUE. Did you use LMDBSerializer.UnsignedLong instead of LMDBSerializer.Long?");
    if (capacity < -1 || 0 == capacity) throw new IllegalArgumentException("Invalid Capacity. Must be -1 or greater than 0: "+capacity);
    this.capacity = capacity;
    this.writeCapacitySemaphore = UNBOUNDED == capacity ? null : new Semaphore(capacity, true);
    this.map = map;
  }

  /**
   * Returns the key to use for prepending to this queue.
   *
   * This should be called within an LMDB Write Transaction for it to be valid
   *
   * @return The key to use for prepending to this queue
   */
  private long nextHeadKey() {
    final Long firstKey = map.firstKey();
    final long nextHeadKey = null == firstKey ? STARTING_KEY : firstKey - 1L;
    if (nextHeadKey == Long.MAX_VALUE) throw new AssertionError("Underflow detected in nextHeadKey. Current lowest key is Long.MIN_VALUE and next value would underflow to Long.MAX_VALUE");
    return nextHeadKey;
  }

  /**
   * Returns the key to use for appending to this queue.
   *
   * This should be called within an LMDB Write Transaction for it to be valid
   *
   * @return The key to use for appending to this queue
   */
  private long nextTailKey() {
    final Long lastKey = map.lastKey();
    final long nextTailKey = null == lastKey ? STARTING_KEY : lastKey + 1L;
    if (nextTailKey == Long.MIN_VALUE) throw new AssertionError("Overflow detected in nextTailKey. Current highest key is Long.MAX_VALUE and next value would overflow to Long.MIN_VALUE");
    return nextTailKey;
  }

  //
  // Write Capacity Acquisition Helpers - Wrappers around the writeCapacitySemaphore to handle when it is null
  //

  private void acquireWriteCapacity() throws InterruptedException {
    if (null == writeCapacitySemaphore) return;
    writeCapacitySemaphore.acquire();
  }

  private void releaseWriteCapacity() {
    if (null == writeCapacitySemaphore) return;
    writeCapacitySemaphore.release();
  }

  private boolean tryAcquireWriteCapacity() {
    if (null == writeCapacitySemaphore) return true;
    return writeCapacitySemaphore.tryAcquire();
  }

  private boolean tryAcquireWriteCapacity(int permits) {
    if (null == writeCapacitySemaphore) return true;
    return writeCapacitySemaphore.tryAcquire(permits);
  }

  private boolean tryAcquireWriteCapacity(long timeout, TimeUnit unit) throws InterruptedException {
    if (null == writeCapacitySemaphore) return true;
    return writeCapacitySemaphore.tryAcquire(timeout, unit);
  }

  //
  // Read Capacity Acquisition Helpers - Wrappers around the readCapacitySemaphore
  //

  private void acquireReadCapacity() throws InterruptedException {
    readCapacitySemaphore.acquire();
  }

  private void releaseReadCapacity() {
    readCapacitySemaphore.release();
  }

  private boolean tryAcquireReadCapacity() {
    return readCapacitySemaphore.tryAcquire();
  }

  private boolean tryAcquireReadCapacity(long timeout, TimeUnit unit) throws InterruptedException {
    return readCapacitySemaphore.tryAcquire(timeout, unit);
  }


  //
  // Add to map helpers
  //

  /**
   * Add to the beginning of the map. Automatically releases read capacity.
   * @param e The element to add
   */
  private void mapAddFirst(E e) {
    if (null == e) {
      releaseWriteCapacity();
      throw new NullPointerException("Cannot add null element to LMDBBlockingQueue");
    }

    // The nextHeadKey() and map.prepend() must be wrapped in the same ReadWrite Transaction
    try (final LMDBTxn txn = map.withReadWriteTxn()) {
      if (!map.prepend(nextHeadKey(), e)) {
        releaseWriteCapacity();
        throw new AssertionError("Expected map.prepend(nextHeadKey(), e) to succeed");
      }
    }

    releaseReadCapacity();
  }

  /**
   * Add to the end of the map. Automatically releases read capacity.
   * @param e The element to add
   */
  private void mapAddLast(E e) {
    if (null == e) {
      releaseWriteCapacity();
      throw new NullPointerException("Cannot add null element to LMDBBlockingQueue");
    }

    // The nextTailKey() and map.append() must be wrapped in the same ReadWrite Transaction
    try (final LMDBTxn txn = map.withReadWriteTxn()) {
      if (!map.append(nextTailKey(), e)) {
        releaseWriteCapacity();
        throw new AssertionError("Expected map.append(nextTailKey(), e) to succeed");
      }
    }

    releaseReadCapacity();
  }

  //
  // Take from map helpers
  //

  /**
   * This is only valid if you have acquired read capacity. This will automatically release the write capacity.
   * @return The non-null element from the beginning of the map
   */
  private E mapTakeFirst() {
    final E value = map.pollFirstValue();
    if (null == value) throw new AssertionError("Since we were able to acquire read capacity we expected map.pollFirstValue to be non-null!");
    releaseWriteCapacity();
    return value;
  }

  /**
   * This is only valid if you have acquired read capacity. This will automatically release the write capacity.
   * @return The non-null element from the end of the map
   */
  private E mapTakeLast() {
    final E value = map.pollLastValue();
    if (null == value) throw new AssertionError("Since we were able to acquire read capacity we expected map.pollLastValue to be non-null!");
    releaseWriteCapacity();
    return value;
  }

  //
  // Add to queue methods
  //

  @Override
  public boolean add(E e) {
    addLast(e);  // addLast will throw an IllegalStateException if there is no space
    return true; // Always returns true or throws an exception above
  }

  @Override
  public void addFirst(E e) {
    if (!offerFirst(e)) throw new IllegalStateException("Queue is full");
  }

  @Override
  public void addLast(E e) {
    if (!offerLast(e)) throw new IllegalStateException("Queue is full");
  }

  @Override
  public boolean offerFirst(E e) {
    if (!tryAcquireWriteCapacity()) return false;
    mapAddFirst(e);
    return true;
  }

  @Override
  public boolean offerLast(E e) {
    if (!tryAcquireWriteCapacity()) return false;
    mapAddLast(e);
    return true;
  }

  @Override
  public void putFirst(E e) throws InterruptedException {
    acquireWriteCapacity();
    mapAddFirst(e);
  }

  @Override
  public void putLast(E e) throws InterruptedException {
    acquireWriteCapacity();
    mapAddLast(e);
  }

  @Override
  public void put(E e) throws InterruptedException {
    putLast(e);
  }

  @Override
  public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
    if (!tryAcquireWriteCapacity(timeout, unit)) return false;
    mapAddFirst(e);
    return true;
  }

  @Override
  public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
    if (!tryAcquireWriteCapacity(timeout, unit)) return false;
    mapAddLast(e);
    return true;
  }

  @Override
  public boolean offer(E e) {
    return offerLast(e);
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    return offerLast(e, timeout, unit);
  }

  @Override
  public void push(E e) {
    addFirst(e);
  }

  //
  // Remove from Queue Methods
  //
  @Override
  public E poll() {
    return pollFirst();
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    return pollFirst(timeout, unit);
  }

  @Override
  public E pollFirst() {
    if (!tryAcquireReadCapacity()) return null;
    return mapTakeFirst();
  }

  @Override
  public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
    if (!tryAcquireReadCapacity(timeout, unit)) return null;
    return mapTakeFirst();
  }

  @Override
  public E pollLast() {
    if (!tryAcquireReadCapacity()) return null;
    return mapTakeLast();
  }

  @Override
  public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
    if (!tryAcquireReadCapacity(timeout, unit)) return null;
    return mapTakeLast();
  }

  @Override
  public E pop() {
    return removeFirst();
  }

  @Override
  public E remove() {
    return removeFirst();
  }

  @Override
  public E removeFirst() {
    final E value = pollFirst();
    if (null == value) throw new NoSuchElementException("Queue is empty");
    return value;
  }

  @Override
  public E removeLast() {
    final E value = pollLast();
    if (null == value) throw new NoSuchElementException("Queue is empty");
    return value;
  }

  @Override
  public E take() throws InterruptedException {
    return takeFirst();
  }

  @Override
  public E takeFirst() throws InterruptedException {
    acquireReadCapacity();
    return mapTakeFirst();
  }

  @Override
  public E takeLast() throws InterruptedException {
    acquireReadCapacity();
    return mapTakeLast();
  }

  //
  // Peek Methods
  //

  @Override
  public E element() {
    return getFirst();
  }

  @Override
  public E peek() {
    return peekFirst();
  }

  @Override
  public E getFirst() {
    final Map.Entry<Long, E> first = map.firstEntry();
    if (null == first) throw new NoSuchElementException("Queue is empty");
    return first.getValue();
  }

  @Override
  public E getLast() {
    final Map.Entry<Long, E> last = map.lastEntry();
    if (null == last) throw new NoSuchElementException("Queue is empty");
    return last.getValue();
  }

  @Override
  public E peekFirst() {
    final Map.Entry<Long, E> first = map.firstEntry();
    return null == first ? null : first.getValue();
  }

  @Override
  public E peekLast() {
    final Map.Entry<Long, E> last = map.lastEntry();
    return null == last ? null : last.getValue();
  }

  //
  // Other Misc Queue/Collection methods
  //


  @Override
  public boolean contains(Object o) {
    return map.containsValue(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return map.values().containsAll(c);
  }

  @Override
  public int remainingCapacity() {
    // Our capacity in a Long but this method returns an int so convert to Integer.MAX_VALUE if we have more
    // capacity than that available.
    return capacity == UNBOUNDED ? Integer.MAX_VALUE : (int)Math.min(capacity - map.valueCount(), Integer.MAX_VALUE);
  }

  @Override
  public boolean remove(Object o) {
    return removeFirstOccurrence(o);
  }

  @Override
  public boolean removeFirstOccurrence(Object o) {
    // Since we are potentially reading from the Queue we need to acquire a permit
    if (!tryAcquireReadCapacity()) return false;

    // Not sure if this code is safe for the read/write semaphores
    try (final LMDBTxn txn = map.withReadWriteTxn(); final LMDBIterator<E> it = map.values().lmdbIterator()) {
      while (it.hasNext()) {
        if (Objects.equals(o, it.next())) {
          it.remove();
          releaseWriteCapacity();
          return true;
        }
      }
    }

    // We didn't actually read anything from the queue so release our read permit back
    releaseReadCapacity();

    return false;
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    // Since we are potentially reading from the Queue we need to acquire a permit
    if (!tryAcquireReadCapacity()) return false;

    // Not sure if this code is safe for the read/write semaphores
    try (final LMDBTxn txn = map.withReadWriteTxn(); final LMDBIterator<E> it = map.descendingMap().values().lmdbIterator()) {
      while (it.hasNext()) {
        if (Objects.equals(o, it.next())) {
          it.remove();
          releaseWriteCapacity();
          return true;
        }
      }
    }

    // We didn't actually read anything from the queue so release our read permit back
    releaseReadCapacity();

    return false;
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    // If the collection is of size Integer.MAX_VALUE then let's just assume we cannot handle it since that can mean it
    // has more than Integer.MAX_VALUE. This is really a limitation of using Semaphore which are restricted to int
    // permit counts. If semaphore used a long then we could handle it. Either way it seems unlikely we would be adding
    // more than Integer.MAX_VALUE elements from another collection.
    if (c.size() == Integer.MAX_VALUE) throw new IllegalStateException("Cannot add Integer.MAX_VALUE (or more) elements to Queue");

    if (!tryAcquireWriteCapacity(c.size())) return false;

    // TODO: we should probably group these into batched transactions (or use a single transaction for all).
    for (final E e : c) {
      mapAddFirst(e);
    }

    return true;
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
  public void clear() {
    // This is very inefficient. Using map.clear() would be more efficient but we would need to figure out how to safely
    // handle the read/write semaphores in case there is concurrent modification.
    while (null != pollLast()) { }
  }

  @Override
  public boolean equals(Object o) {
    // Use the default implementation
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    // Use the default implementation
    return super.hashCode();
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    int count = 0;

    // This is not efficient since we do it one-by-one but it should at least be accurate for the read/write semaphores
    while (tryAcquireReadCapacity()) {
      c.add(mapTakeLast());
      count += 1;
    }

    return count;
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    int count = 0;

    // This is not efficient since we do it one-by-one but it should at least be accurate for the read/write semaphore
    while (count < maxElements && tryAcquireReadCapacity()) {
      c.add(mapTakeLast());
      count += 1;
    }

    return count;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public Iterator<E> iterator() {
    return map.values().iterator();
  }

  @Override
  public Object[] toArray() {
    return map.values().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return map.values().toArray(a);
  }

  @Override
  public Iterator<E> descendingIterator() {
    return map.descendingMap().values().iterator();
  }
}