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
import java.util.Objects;

class LMDBMultiSetView<V> extends LMDBMultiSetInternal<V> {
  private static enum CeilingMode {
    NoPossibleMatch, UseHigher, UseCeiling
  }

  private static enum FloorMode {
    NoPossibleMatch, UseLower, UseFloor
  }

  public static <V> LMDBMultiSetView<V> headSet(LMDBMultiSetImpl<?, V> set, V toValue, boolean toInclusive) {
    return new LMDBMultiSetView<V>(set, null, false, toValue, toInclusive);
  }

  public static <V> LMDBMultiSetView<V> subSet(LMDBMultiSetImpl<?, V> set, V fromValue, boolean fromInclusive, V toValue, boolean toInclusive) {
    return new LMDBMultiSetView<V>(set, fromValue, fromInclusive, toValue, toInclusive);
  }

  public static <V> LMDBMultiSetView<V> tailSet(LMDBMultiSetImpl<?, V> set, V fromValue, boolean fromInclusive) {
    return new LMDBMultiSetView<V>(set, fromValue, fromInclusive, null, false);
  }

  private final LMDBMultiSetImpl<?, V> set;
  private final V fromValue;
  private final boolean fromInclusive;
  private final V toValue;
  private final boolean toInclusive;
  private final ByteBuffer fromValueBuf;
  private final ByteBuffer toValueBuf;

  LMDBMultiSetView(LMDBMultiSetImpl<?, V> set, V fromValue, boolean fromInclusive, V toValue, boolean toInclusive) {
    this.set = set;
    this.fromValue = fromValue;
    this.fromInclusive = fromInclusive;
    this.toValue = toValue;
    this.toInclusive = toInclusive;
    fromValueBuf = null != fromValue ? set.valueSerializer().serialize(fromValue, null) : null;
    toValueBuf = null != toValue ? set.valueSerializer().serialize(toValue, null) : null;
  }

  @Override
  public boolean add(V e) {
    return withinRange(e) && set.add(e);
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    boolean modified = false;

    try (LMDBTxnInternal txn = withNestedReadWriteTxn()) {
      for (V v : c) {
        rangeCheck(v);
        if (set.add(v)) modified = true;
      }
    }

    return modified;
  }

  /**
   * @return fromValue if key &lt; fromValue, toValue if key &gt; toValue,
   *         otherwise key unchanged
   */
  protected V adjustValue(V value) {
    if (null != fromValue && compare(value, null, fromValue, fromValueBuf) < 0) return fromValue;
    if (null != toValue && compare(value, null, toValue, toValueBuf) > 0) return toValue;
    return value;
  }

  @Override
  V ceiling(ByteBuffer buf) {
    return set.ceiling(buf);
  }

  @Override
  public V ceiling(V value) {
    value = adjustValue(value);

    switch (ceilingMode(value)) {
      case NoPossibleMatch:
        return null;
      case UseHigher:
        return handleNavigableValueResult(set.higher(value));
      case UseCeiling:
        return handleNavigableValueResult(set.ceiling(value));
      default:
        throw new IllegalStateException("Unknown Enum Value");
    }
  }

  private CeilingMode ceilingMode(V value) {
    if (!toInclusive && Objects.equals(value, toValue)) return CeilingMode.NoPossibleMatch;
    if (!fromInclusive && Objects.equals(value, fromValue)) return CeilingMode.UseHigher;
    return CeilingMode.UseCeiling;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not currently supported for subsets");
  }

  @Override
  public void close() {
    // Nothing to close
  }

  @Override
  public Comparator<? super V> comparator() {
    return set.comparator();
  }

  @Override
  public int compare(V a, ByteBuffer aBuf, V b, ByteBuffer bBuf) {
    return set.compare(a, aBuf, b, bBuf);
  }

  @Override
  public int compare(V a, V b) {
    return set.compare(a, b);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(Object o) {
    return withinRange((V) o) && set.contains(o);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean containsAll(Collection<?> c) {
    try (LMDBTxnInternal txn = withReadOnlyTxn()) {
      for (Object v : c) {
        if (!withinRange((V) v) || !contains(v)) return false;
      }
    }

    return true;
  }

  @Override
  public Iterator<V> descendingIterator() {
    return new LMDBIteratorImpl<V>(newCursorAdapter(LMDBCursor.Mode.USE_EXISTING_TXN).reversed());
  }

  @Override
  public LMDBIterator<V> descendingLMDBIterator() {
    return new LMDBIteratorImpl<V>(newCursorAdapter(LMDBCursor.Mode.READ_ONLY).reversed());
  }

  @Override
  public LMDBMultiSet<V> descendingSet() {
    return new LMDBMultiSetReversed<V>(this);
  }

  @Override
  LMDBEnvInternal env() {
    return set.env();
  }

  @Override
  public V first() {
    if (null == fromValue) return set.first();
    final V res = fromInclusive ? set.ceiling(fromValueBuf) : set.higher(fromValueBuf);
    if (null != res && !withinRange(res)) return null;
    return res;
  }

  @Override
  V floor(ByteBuffer buf) {
    return set.floor(buf);
  }

  @Override
  public V floor(V value) {
    value = adjustValue(value);

    switch (floorMode(value)) {
      case NoPossibleMatch:
        return null;
      case UseLower:
        return handleNavigableValueResult(set.lower(value));
      case UseFloor:
        return handleNavigableValueResult(set.floor(value));
      default:
        throw new IllegalStateException("Unknown Enum Value");
    }
  }

  private FloorMode floorMode(V value) {
    if (!fromInclusive && Objects.equals(value, fromValue)) return FloorMode.NoPossibleMatch;
    if (!toInclusive && Objects.equals(value, toValue)) return FloorMode.UseLower;
    return FloorMode.UseFloor;
  }

  private V handleNavigableValueResult(V value) {
    return null != value && withinRange(value) ? value : null;
  }

  @Override
  public LMDBMultiSet<V> headSet(V toValue) {
    return headSet(toValue, false);
  }

  @Override
  public LMDBMultiSet<V> headSet(V toValue, boolean toInclusive) {
    return subSet(fromValue, fromInclusive, toValue, toInclusive);
  }

  @Override
  V higher(ByteBuffer buf) {
    return set.higher(buf);
  }

  @Override
  public V higher(V value) {
    final V res = set.higher(adjustValue(value));
    if (null != res && !withinRange(res)) return null;
    return res;
  }

  @Override
  public boolean isEmpty() {
    try (LMDBIterator<V> it = lmdbIterator()) {
      return !it.hasNext();
    }
  }

  @Override
  public Iterator<V> iterator() {
    return new LMDBIteratorImpl<V>(newCursorAdapter(LMDBCursor.Mode.USE_EXISTING_TXN));
  }

  @Override
  public V last() {
    if (null == toValue) return set.last();
    final V res = toInclusive ? set.floor(toValueBuf) : set.lower(toValueBuf);
    if (null != res && !withinRange(res)) return null;
    return res;
  }

  @Override
  public LMDBIterator<V> lmdbIterator() {
    return new LMDBIteratorImpl<V>(newCursorAdapter(LMDBCursor.Mode.READ_ONLY));
  }

  private LMDBCursorAdapter<V> newCursorAdapter(LMDBCursor.Mode mode) {
    final LMDBMultiSetImpl.MultiSetCursor<V> parent = set.openCursor(mode);

    return new LMDBCursorAdapter<V>() {
      public V first() {
        if (null == fromValue) return parent.first();
        final V res = fromInclusive ? parent.ceiling(fromValueBuf) : parent.higher(fromValueBuf);
        return null != res && withinRange(res) ? res : null;
      }

      public V last() {
        if (null == toValue) return parent.last();
        final V res = toInclusive ? parent.floor(toValueBuf) : parent.lower(toValueBuf);
        return null != res && withinRange(res) ? res : null;
      }

      public V next() {
        final V res = parent.next();
        return null != res && withinRange(res) ? res : null;
      }

      public V prev() {
        final V res = parent.prev();
        return null != res && withinRange(res) ? res : null;
      }

      public void delete() {
        parent.delete();
      }

      public boolean readOnly() {
        return parent.readOnly();
      }

      public void close() {
        parent.close();
      }
    };
  }

  @Override
  V lower(ByteBuffer buf) {
    return set.lower(buf);
  }

  @Override
  public V lower(V value) {
    final V res = set.lower(adjustValue(value));
    if (null != res && !withinRange(res)) return null;
    return res;
  }

  @Override
  public V pollFirst() {
    if (null == fromValue) return set.pollFirst();
    try (LMDBTxnInternal txn = withReadWriteTxn()) {
      final V res = first();
      if (null != res) remove(res);
      return res;
    }
  }

  @Override
  public V pollLast() {
    if (null == toValue) return set.pollLast();
    try (LMDBTxnInternal txn = withReadWriteTxn()) {
      final V res = last();
      if (null != res) remove(res);
      return res;
    }
  }

  protected void rangeCheck(V value) {
    rangeCheck(value, true);
  }

  protected void rangeCheck(V value, boolean inclusive) {
    rangeCheck(value, null, inclusive);
  }

  protected void rangeCheck(V value, ByteBuffer valueBuf) {
    rangeCheck(value, valueBuf, true);
  }

  protected void rangeCheck(V value, ByteBuffer valueBuf, boolean inclusive) {
    if (!withinRange(value, valueBuf, inclusive)) throw new LMDBOutOfRangeException("Value out of range: " + value + "  fromValue: " + fromValue + " fromInclusive: " + fromInclusive + "  toValue: " + toValue + "  toInclusive: " + toInclusive);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object o) {
    rangeCheck((V) o);
    return set.remove(o);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;

    try (LMDBTxnInternal txn = withNestedReadWriteTxn()) {
      for (Object v : c) {
        rangeCheck((V) v);
        if (set.remove(v)) modified = true;
      }
    }

    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public int size() {
    int count = 0;

    try (LMDBIterator<V> it = lmdbIterator()) {
      while (it.hasNext()) {
        it.next();
        count++;
      }
    }

    return count;
  }

  @Override
  public LMDBMultiSet<V> subSet(V fromValue, boolean fromInclusive, V toValue, boolean toInclusive) {
    if (null != fromValue && null != toValue && compare(fromValue, toValue) >= 0) throw new IllegalArgumentException("Expected fromValue to be less than toValue");
    return new LMDBMultiSetView<V>(set, adjustValue(fromValue), fromInclusive && (null == fromValue || this.fromInclusive), adjustValue(toValue), toInclusive && (null == toValue || this.toInclusive));
  }

  @Override
  public LMDBMultiSet<V> subSet(V fromValue, V toValue) {
    return subSet(fromValue, true, toValue, false);
  }

  @Override
  public LMDBMultiSet<V> tailSet(V fromValue) {
    return tailSet(fromValue, true);
  }

  @Override
  public LMDBMultiSet<V> tailSet(V fromValue, boolean fromInclusive) {
    return subSet(fromValue, fromInclusive, toValue, toInclusive);
  }

  @Override
  LMDBSerializer<V> valueSerializer() {
    return set.valueSerializer();
  }

  protected boolean withinRange(V value) {
    return withinRange(value, true);
  }

  protected boolean withinRange(V value, boolean inclusive) {
    return withinRange(value, null, inclusive);
  }

  protected boolean withinRange(V value, ByteBuffer valueBuf) {
    return withinRange(value, valueBuf, true);
  }

  protected boolean withinRange(V value, ByteBuffer valueBuf, boolean inclusive) {
    if (null == value) return false;

    boolean isGood = true;

    if (null != fromValue) {
      final int ret = set.compare(fromValue, fromValueBuf, value, valueBuf);
      isGood = isGood && (ret < 0 || (0 == ret && (fromInclusive || !inclusive)));
    }

    if (null != toValue && isGood) {
      final int ret = set.compare(value, valueBuf, toValue, toValueBuf);
      isGood = isGood && (ret < 0 || (0 == ret && (toInclusive || !inclusive)));
    }

    return isGood;
  }
}
