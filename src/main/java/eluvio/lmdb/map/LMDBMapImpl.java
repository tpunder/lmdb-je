package eluvio.lmdb.map;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import eluvio.lmdb.api.Api;
import eluvio.lmdb.api.Cursor;
import eluvio.lmdb.api.DB;
import eluvio.lmdb.api.Txn;
import eluvio.lmdb.api.Cursor.KeyAndData;

class LMDBMapImpl<K,V> extends LMDBMapInternal<K,V> {
  
  final boolean dup;
  final LMDBEnvInternal env;
  private DB db;
  final LMDBSerializer<K> keySerializer;
  final LMDBSerializer<V> valueSerializer;
  private final Comparator<K> keyComparator;
  private final Comparator<V> valueComparator;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private final LMDBMapInternal<K,V> reversed;
  private final LMDBKeySet<K> keySet;
  private final LMDBEntrySet<K,V> entrySet;
  private final LMDBValuesCollection<V> values;
  
  private final Comparator<V> externalValueComparator = new Comparator<V>() {
    @Override
    public int compare(V a, V b) {
      return valueCompare(a, b);
    }
  };
  
  /**
   * A re-usable ByteBuffer that can be used for serializing keys
   */
  private final ThreadLocal<ReusableBuf> cachedKeyBuffer = new ThreadLocal<ReusableBuf>() {
    @Override
    protected ReusableBuf initialValue() {
      ByteBuffer buf = ByteBuffer.allocateDirect(keySerializer.cachedBufferSize() > 0 ? keySerializer.cachedBufferSize() : env.env().maxKeySize());
      return new ReusableBuf(buf);
    }
  };
  
  /**
   * A re-usable ByteBuffer that can be used for serializing values
   */
  private final ThreadLocal<ReusableBuf> cachedValueBuffer = new ThreadLocal<ReusableBuf>() {
    @Override
    protected ReusableBuf initialValue() {
      final int defaultCachedValueBufferSize = 4096; // TODO: make this configurable
      
      ByteBuffer buf = null;
      int cachedValueBufferSize = 0;
      
      final int maxKeySize = env.env().maxKeySize();
      final int serializerCachedSize = valueSerializer.cachedBufferSize();
      
      if (dup) cachedValueBufferSize = serializerCachedSize > 0 && serializerCachedSize < maxKeySize ? serializerCachedSize : maxKeySize;
      else if (serializerCachedSize > 0) cachedValueBufferSize = serializerCachedSize;
      else if (defaultCachedValueBufferSize > 0) cachedValueBufferSize = defaultCachedValueBufferSize;
      
      if (cachedValueBufferSize > 0) buf = ByteBuffer.allocateDirect(cachedValueBufferSize);
      
      return new ReusableBuf(buf);
    }
  };

  private static class WrappedByteBufferComparator<T> implements Comparator<ByteBuffer> {
    private final Comparator<T> comparator;
    private final LMDBSerializer<T> serializer;
    
    public WrappedByteBufferComparator(Comparator<T> comparator, LMDBSerializer<T> serializer) {
      this.comparator = comparator;
      this.serializer = serializer;
    }
    
    @Override
    public int compare(ByteBuffer a, ByteBuffer b) {
      return comparator.compare(serializer.deserialize(a), serializer.deserialize(b));
    }
  }
  
  private static class ByteBufferComparator implements Comparator<ByteBuffer> {
    private final LMDBComparator comparator;
    
    public ByteBufferComparator(LMDBComparator comparator) {
      this.comparator = comparator;
    }
    
    @Override
    public int compare(ByteBuffer a, ByteBuffer b) {
      return comparator.compare(a, b);
    }
  }
  
  LMDBMapImpl(LMDBEnvInternal env, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer) {
    this(env, keySerializer, valueSerializer, null);
  }
  
  LMDBMapImpl(LMDBEnvInternal env, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator) {
    this(env, keySerializer, valueSerializer, keyComparator, false);
  }
  
  LMDBMapImpl(LMDBEnvInternal env, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, boolean dup) {
    this(env, keySerializer, valueSerializer, keyComparator, null, dup);
  }
  
  LMDBMapImpl(LMDBEnvInternal env, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, Comparator<V> valueComparator, boolean dup) {
    this.dup = dup;
    this.env = env;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyComparator = keyComparator;
    this.valueComparator = valueComparator;

    final int readOnlyFlag = env.readOnly() ? Api.MDB_RDONLY : 0;
        
    Txn txn = env.env().beginTxn(readOnlyFlag);
    
    final Comparator<ByteBuffer> comparator = null != keyComparator ? keyComparator instanceof LMDBComparator ? new ByteBufferComparator((LMDBComparator)keyComparator) : new WrappedByteBufferComparator<K>(keyComparator, keySerializer) : null;
    final Comparator<ByteBuffer> dupComparator = null != valueComparator ? valueComparator instanceof LMDBComparator ? new ByteBufferComparator((LMDBComparator)valueComparator) : new WrappedByteBufferComparator<V>(valueComparator, valueSerializer) : null;
    
    int dbFlags = keySerializer.integerKeys() ? Api.MDB_INTEGERKEY : 0;
    
    if (dup) {
      dbFlags = dbFlags | Api.MDB_DUPSORT;
      if (valueSerializer.integerKeys()) dbFlags = dbFlags | Api.MDB_INTEGERDUP;
      if (valueSerializer.fixedSize()) dbFlags = dbFlags | Api.MDB_DUPFIXED;
    }
    
    final String name = null; // TODO: add future support for named databases
    
    db = new DB(txn, name, dbFlags, comparator, dupComparator);
    
    txn.commit();
    
    reversed = new LMDBMapReversed<K,V>(this);
    keySet = new LMDBKeySet<K>(this);
    entrySet = new LMDBEntrySet<K,V>(this);
    values = new LMDBValuesCollection<V>(this);
  }
  
  boolean dup() {
    return dup;
  }
  
  @Override
  LMDBSerializer<K> keySerializer() {
    return keySerializer;
  }
  
  @Override
  LMDBSerializer<V> valueSerializer() {
    return valueSerializer;
  }
  
  ReusableBuf withCachedKeyBuf() {
    ReusableBuf buf = cachedKeyBuffer.get();
    buf.open();
    return buf;
  }
  
  ReusableBuf withCachedValueBuf() {
    ReusableBuf buf = cachedValueBuffer.get();
    buf.open();
    return buf;
  }

  @Override
  public int compare(K a, K b) {
    return compare(a, null, b, null);
  }
  
  @Override
  public int compare(K a, ByteBuffer aBuf, K b, ByteBuffer bBuf) {
    // If both are null then they are equal otherwise we sort nulls first
    if (null == a) return null == b ? 0 : -1;
    if (null == b) return null == a ? 0 : 1;
    
    if (null != keyComparator) {
      return keyComparator.compare(a, b);
    } else {
      if (null == aBuf) aBuf = keySerializer.serialize(a, null);
      if (null == bBuf) bBuf = keySerializer.serialize(b, null);
      return compareUsingDB(aBuf, bBuf);
    }
  }
  
  private int compareUsingDB(ByteBuffer a, ByteBuffer b) {
    try(LMDBTxnInternal txn = withReadOnlyTxn()) {
      return db.compare(txn.txn(),  a, b);
    }
  }
  
  @Override
  public Comparator<V> valueComparator() {
    return dup ? externalValueComparator : null;
  }
  
  @Override
  public int valueCompare(V a, V b) {
    return valueCompare(a, null, b, null);
  }
  
  @Override
  public int valueCompare(V a, ByteBuffer aBuf, V b, ByteBuffer bBuf) {
    if (!dup) throw new IllegalArgumentException("This method only works if this is a MDB_DUPSORT database");
    
    // If both are null then they are equal otherwise we sort nulls first
    if (null == a) return null == b ? 0 : -1;
    if (null == b) return null == a ? 0 : 1;
    
    if (null != valueComparator) {
      return valueComparator.compare(a, b);
    } else {
      if (null == aBuf) aBuf = valueSerializer.serialize(a, null);
      if (null == bBuf) bBuf = valueSerializer.serialize(b, null);
      return valueCompareUsingDB(aBuf, bBuf);
    }
  }
  
  private int valueCompareUsingDB(ByteBuffer a, ByteBuffer b) {
    try(LMDBTxnInternal txn = withReadOnlyTxn()) {
      return db.dupCompare(txn.txn(),  a, b);
    }
  }
  
  private Map.Entry<K,V> toMapEntry(KeyAndData pair) {
    return null != pair ? new AbstractMap.SimpleImmutableEntry<K,V>(toKey(pair), toValue(pair)) : null;
  }
  
  private K toKey(KeyAndData pair) {
    return null != pair && null != pair.key ? keySerializer.deserialize(pair.key) : null;
  }
  
  private V toValue(KeyAndData pair) {
    return null != pair && null != pair.data ? valueSerializer.deserialize(pair.data) : null;
  }
  
  @Override
  CursorImpl openCursor(LMDBCursor.Mode mode) {
    return new CursorImpl(mode);
  }
  
  class CursorImpl implements LMDBCursor<K,V> {
    private final LMDBTxnInternal txn;
    private final Cursor cursor;
    private AtomicBoolean cursorClosed = new AtomicBoolean(false);
    
    public CursorImpl(LMDBCursor.Mode mode) {
      switch(mode) {
        case READ_ONLY:
          txn = withReadOnlyTxn();
          break;
        case READ_WRITE:
          txn = withReadWriteTxn();
          break;
        case USE_EXISTING_TXN:
          txn = withExistingTxn();
          break;
        default:
          throw new IllegalArgumentException("Invalid CursorMode: "+mode);
      }

      cursor = db.openCursor(txn.txn());
    }
    
    @Override
    public boolean readOnly() { return txn.readOnly(); }
    
    @Override
    public boolean moveTo(K key, ByteBuffer keyBuf) {
      return cursor.moveTo(keyBuf);
    }
    
    @Override
    public boolean moveTo(K key, ByteBuffer keyBuf, V value, ByteBuffer valueBuf) {
      return cursor.moveTo(keyBuf, valueBuf);
    }
    
    @Override
    public void delete() {
      cursor.delete();
    }
    
    @Override
    public Map.Entry<K,V> first() { return toMapEntry(cursor.first()); }
    
    @Override
    public Map.Entry<K,V> last()  { return toMapEntry(cursor.last());  }
    
    @Override
    public Map.Entry<K,V> next()  { return toMapEntry(cursor.next());  }
    
    @Override
    public Map.Entry<K,V> prev()  { return toMapEntry(cursor.prev());  }
    
    public Map.Entry<K,V> ceiling(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return ceiling(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public Map.Entry<K,V> higher(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return toMapEntry(cursor.higher(keySerializer.serialize(key, cachedKeyBuf.buf)));
      }
    }
    
    public Map.Entry<K,V> floor(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return toMapEntry(cursor.floor(keySerializer.serialize(key, cachedKeyBuf.buf)));
      }
    }
    
    public Map.Entry<K,V> lower(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return toMapEntry(cursor.lower(keySerializer.serialize(key, cachedKeyBuf.buf)));
      }
    }
    
    public Map.Entry<K,V> ceiling(ByteBuffer key) { return toMapEntry(cursor.ceiling(key)); }
    public Map.Entry<K,V> higher(ByteBuffer key)  { return toMapEntry(cursor.higher(key));  }
    public Map.Entry<K,V> floor(ByteBuffer key)   { return toMapEntry(cursor.floor(key));   }
    public Map.Entry<K,V> lower(ByteBuffer key)   { return toMapEntry(cursor.lower(key));   }
    
    @Override
    public K firstKey() { return toKey(cursor.first()); }
    
    @Override
    public K lastKey()  { return toKey(cursor.last());  }
    
    @Override
    public K nextKey()  { return toKey(cursor.nextNoDup());  }
    
    @Override
    public K prevKey()  { return toKey(cursor.prevNoDup());  }
    
    public K ceilingKey(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return ceilingKey(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public K higherKey(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return higherKey(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public K floorKey(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return floorKey(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public K lowerKey(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return lowerKey(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public K ceilingKey(ByteBuffer key) { return toKey(cursor.ceiling(key)); }
    public K higherKey(ByteBuffer key)  { return toKey(cursor.higher(key));  }
    public K floorKey(ByteBuffer key)   { return toKey(cursor.floor(key));    }
    public K lowerKey(ByteBuffer key)   { return toKey(cursor.lower(key));    }
    
    @Override
    public V firstValue() { return toValue(cursor.first()); }
    @Override
    public V lastValue()  { return toValue(cursor.last());  }
    @Override
    public V nextValue()  { return toValue(cursor.next());  }
    @Override
    public V prevValue()  { return toValue(cursor.prev());  }
    
    @Override
    public V firstDupValue() { return toValue(cursor.firstDup()); }
    @Override
    public V lastDupValue()  { return toValue(cursor.lastDup());  }
    @Override
    public V nextDupValue()  { return toValue(cursor.nextDup());  }
    @Override
    public V prevDupValue()  { return toValue(cursor.prevDup());  }
    
    @Override
    public long dupCount() { return cursor.dupCount(); }
    
    @Override
    public V dupCeiling(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return toValue(cursor.dupCeiling(keyBuf, valueBuf));
    }
    
    @Override
    public V dupFloor(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return toValue(cursor.dupFloor(keyBuf, valueBuf));
    }
    
    @Override
    public V dupHigher(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return toValue(cursor.dupHigher(keyBuf, valueBuf));
    }
    
    @Override
    public V dupLower(ByteBuffer keyBuf, ByteBuffer valueBuf) {
      return toValue(cursor.dupLower(keyBuf, valueBuf));
    }
    
    public V ceilingValue(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return ceilingValue(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public V higherValue(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return higherValue(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public V floorValue(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return floorValue(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public V lowerValue(K key) {
      try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
        return lowerValue(keySerializer.serialize(key, cachedKeyBuf.buf));
      }
    }
    
    public V ceilingValue(ByteBuffer key) { return toValue(cursor.ceiling(key)); }
    public V higherValue(ByteBuffer key)  { return toValue(cursor.higher(key));  }
    public V floorValue(ByteBuffer key)   { return toValue(cursor.floor(key));   }
    public V lowerValue(ByteBuffer key)   { return toValue(cursor.lower(key));   }
    
    @Override
    public void close() {
      if (cursorClosed.compareAndSet(false, true)) {
        cursor.close();
        txn.close();  
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //
  // LMDBMap implementation
  //
  ///////////////////////////////////////////////////////////////////////////
  
  @Override
  LMDBEnvInternal env() {
    return env;
  }
  
  @Override
  public K pollFirstKey() {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.first();
      if (null == res) return null;
      K key = toKey(res);
      cursor.delete();
      return key;
    }
  }
  
  @Override
  public K pollLastKey() {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.last();
      if (null == res) return null;
      K key = toKey(res);
      cursor.delete();
      return key;
    }
  }
  
  @Override
  public boolean contains(K key, V value) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      return contains(key, keyBuf, value);
    }
  }
  
  @Override
  boolean contains(K key, ByteBuffer keyBuf, V value) {
    return dup ? containsDup(key, keyBuf, value) : containsNoDup(key, keyBuf, value);
  }
  
  private boolean containsNoDup(K key, ByteBuffer keyBuf, V value) {
    try (LMDBTxnInternal txn = withReadOnlyTxn()) {
      V existing = get(txn.txn(), keyBuf);
      return null != existing && Objects.equals(existing, value);
    }
  }
  
  private boolean containsDup(K key, ByteBuffer keyBuf, V value) {
    try (
      ReusableBuf cachedValueBuf = withCachedValueBuf();
      LMDBCursor<K,V> cursor = openReadOnlyCursor()
    ) {
      final ByteBuffer valueBuf = valueSerializer.serialize(value, cachedValueBuf.buf);
      return cursor.moveTo(key, keyBuf, value, valueBuf);
    }
  }
  
  ///////////////////////////////////////////////////////////////////////////
  //
  // ConcurrentNavigableMap implementation
  //
  ///////////////////////////////////////////////////////////////////////////
  
  @Override
  public LMDBKeySet<K> descendingKeySet() {
    return descendingMap().keySet();
  }
  
  @Override
  public LMDBMapInternal<K,V> descendingMap() {
    return reversed;
  }
  
  @Override
  public LMDBMapInternal<K,V> headMap(K toKey) {
    return headMap(toKey, false);
  }
  
  @Override
  public LMDBMapInternal<K,V> headMap(K toKey, boolean toInclusive) {
    return LMDBMapView.headMap(this, toKey, toInclusive);
  }
  
  @Override
  public LMDBKeySet<K> keySet() {
    return keySet;
  }
  
  @Override
  public LMDBKeySet<K> navigableKeySet() {
    return keySet;
  }
  
  @Override
  public LMDBMapInternal<K,V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return LMDBMapView.subMap(this, fromKey, fromInclusive, toKey, toInclusive);
  }
  
  @Override
  public LMDBMapInternal<K,V> subMap(K fromKey, K toKey) {
    return subMap(fromKey, true, toKey, false);
  }
  
  @Override
  public LMDBMapInternal<K,V> tailMap(K fromKey) {
    return tailMap(fromKey, true);
  }
  
  @Override
  public LMDBMapInternal<K,V> tailMap(K fromKey, boolean fromInclusive) {
    return LMDBMapView.tailMap(this, fromKey, fromInclusive);
  }
  
  ///////////////////////////////////////////////////////////////////////////
  //
  // ConcurrentMap implementation
  //
  ///////////////////////////////////////////////////////////////////////////
  
  @Override
  public V putIfAbsent(K key, V value) {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      ReusableBuf cachedKeyBuf = withCachedKeyBuf();
      ReusableBuf cachedValueBuf = withCachedValueBuf()
    ) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      final ByteBuffer existingValueBuf = db.get(txn.txn(), keyBuf);
      
      if (null != existingValueBuf) return valueSerializer.deserialize(existingValueBuf);
      
      db.put(txn.txn(), keyBuf, valueSerializer.serialize(value, cachedValueBuf.buf));
      
      return null;
    }
  }    
  
  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object key, Object value) {
    return dup ? removeDup((K)key, (V)value) : removeNoDup((K)key, (V)value);
  }
  
  @Override
  boolean remove(K key, ByteBuffer keyBuf, V value) {
    return dup ? removeDup(key, keyBuf, value) : removeNoDup(key, keyBuf, value);
  }
  
  private boolean removeNoDup(K key, V value) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      return removeNoDup(key, keyBuf, value);
    }
  }
  
  private boolean removeNoDup(K key, ByteBuffer keyBuf, V value) {
    try (LMDBTxnInternal txn = withReadWriteTxn()) {
      final V existing = get(txn.txn(), keyBuf);
      if (null != existing && Objects.equals(existing, value)) {
        db.delete(txn.txn(), keyBuf);
        return true;
      } else {
        return false;
      }
    }
  }
  
  private boolean removeDup(K key, V value) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      return removeDup(key, keyBuf, value);
    }
  }
  
  private boolean removeDup(K key, ByteBuffer keyBuf, V value) {
    try (
      ReusableBuf cachedValueBuf = withCachedValueBuf();
      LMDBCursor<K,V> cursor = openReadWriteCursor()
    ) {
      final ByteBuffer valueBuf = valueSerializer.serialize(value, cachedValueBuf.buf);
      
      if (cursor.moveTo(key, keyBuf, value, valueBuf)) {
        cursor.delete();
        return true;
      } else {
        return false;
      }
    }
  }
  
  @Override
  public V replace(K key, V value) {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      ReusableBuf cachedKeyBuf = withCachedKeyBuf();
      ReusableBuf cachedValueBuf = withCachedValueBuf()
    ) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      final ByteBuffer existingValueBuf = db.get(txn.txn(), keyBuf);
      
      if (null == existingValueBuf) return null;
      
      // Must call *before* we call db.put since it might overwrite
      // the existingValueBuf data.
      final V existingValue = valueSerializer.deserialize(existingValueBuf);
      
      db.put(txn.txn(), keyBuf, valueSerializer.serialize(value, cachedValueBuf.buf));
      
      return existingValue;
    }
  }
  
  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      ReusableBuf cachedKeyBuf = withCachedKeyBuf();
      ReusableBuf cachedValueBuf = withCachedValueBuf()
    ) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      final ByteBuffer existingValueBuf = db.get(txn.txn(), keyBuf);
          
      if (null == existingValueBuf) return false;
      
      final V existingValue = valueSerializer.deserialize(existingValueBuf);
      
      if (Objects.equals(existingValue, oldValue)) {
        db.put(txn.txn(), keyBuf, valueSerializer.serialize(newValue, cachedValueBuf.buf));
        return true;
      } else {
        return false;
      }
    }
  }
  
  ///////////////////////////////////////////////////////////////////////////
  //
  // NavigableMap Implementation
  //
  ///////////////////////////////////////////////////////////////////////////
  
  @Override
  public Map.Entry<K,V> ceilingEntry(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      return ceilingEntry(keySerializer.serialize(key, cachedKeyBuf.buf));
    }
  }
  
  Map.Entry<K,V> ceilingEntry(ByteBuffer key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.ceiling(key);
      return null != res ? toMapEntry(res) : null;
    }
  }
  
  @Override
  public K ceilingKey(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      return ceilingKey(keySerializer.serialize(key, cachedKeyBuf.buf));
    }
  }
  
  K ceilingKey(ByteBuffer key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.ceiling(key);
      return null != res ? toKey(res) : null;
    }
  }
  
  @Override
  public Map.Entry<K,V> firstEntry() {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.first();
      return null != res ? toMapEntry(res) : null;
    }
  }
  
  @Override
  public Map.Entry<K,V> floorEntry(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      return floorEntry(keySerializer.serialize(key, cachedKeyBuf.buf));
    }
  }
  
  Map.Entry<K,V> floorEntry(ByteBuffer key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.floor(key);
      return null != res ? toMapEntry(res) : null;
    }
  }
  
  @Override
  public K floorKey(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      return floorKey(keySerializer.serialize(key, cachedKeyBuf.buf));
    }
  }
  
  K floorKey(ByteBuffer key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.floor(key);
      return null != res ? toKey(res) : null;
    }
  }
  
  @Override
  public Map.Entry<K,V> higherEntry(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      return higherEntry(keySerializer.serialize(key, cachedKeyBuf.buf));
    }
  }
  
  Map.Entry<K,V> higherEntry(ByteBuffer key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.higher(key);
      return null != res ? toMapEntry(res) : null;
    }
  }
  
  @Override
  public K higherKey(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      return higherKey(keySerializer.serialize(key, cachedKeyBuf.buf));
    }
  }
  
  K higherKey(ByteBuffer key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.higher(key);
      return null != res ? toKey(res) : null;
    }
  }
  
  @Override
  public Map.Entry<K,V> lastEntry() {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.last();
      return null != res ? toMapEntry(res) : null;
    }
  }
  
  @Override
  public Map.Entry<K,V> lowerEntry(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      return lowerEntry(keySerializer.serialize(key, cachedKeyBuf.buf));
    }
  }
  
  Map.Entry<K,V> lowerEntry(ByteBuffer key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.lower(key);
      return null != res ? toMapEntry(res) : null;
    }
  }
  
  @Override
  public K lowerKey(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      return lowerKey(keySerializer.serialize(key, cachedKeyBuf.buf));
    }
  }
  
  K lowerKey(ByteBuffer key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.lower(key);
      return null != res ? toKey(res) : null;
    }
  }
  
  @Override
  V dupCeiling(K key, ByteBuffer keyBuf, V value) {
    try (ReusableBuf cachedValueBuf = withCachedKeyBuf()) {
      return dupCeiling(key, keyBuf, valueSerializer.serialize(value, cachedValueBuf.buf));
    }
  }
  
  @Override
  V dupFloor(K key, ByteBuffer keyBuf, V value) {
    try (ReusableBuf cachedValueBuf = withCachedKeyBuf()) {
      return dupFloor(key, keyBuf, valueSerializer.serialize(value, cachedValueBuf.buf));
    }
  }
  
  @Override
  V dupHigher(K key, ByteBuffer keyBuf, V value) {
    try (ReusableBuf cachedValueBuf = withCachedKeyBuf()) {
      return dupHigher(key, keyBuf, valueSerializer.serialize(value, cachedValueBuf.buf));
    }
  }
  
  @Override
  V dupLower(K key, ByteBuffer keyBuf, V value) {
    try (ReusableBuf cachedValueBuf = withCachedKeyBuf()) {
      return dupLower(key, keyBuf, valueSerializer.serialize(value, cachedValueBuf.buf));
    }
  }
  
  @Override
  V dupCeiling(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.dupCeiling(keyBuf, valueBuf);
      if (null == res) return null;
      return toValue(res);
    }
  }
  
  @Override
  V dupFloor(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.dupFloor(keyBuf, valueBuf);
      if (null == res) return null;
      return toValue(res);
    }
  }
  
  @Override
  V dupHigher(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.dupHigher(keyBuf, valueBuf);
      if (null == res) return null;
      return toValue(res);
    }
  }
  
  @Override
  V dupLower(K key, ByteBuffer keyBuf, ByteBuffer valueBuf) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.dupLower(keyBuf, valueBuf);
      if (null == res) return null;
      return toValue(res);
    }
  }
  
  @Override
  public Map.Entry<K,V> pollFirstEntry() {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.first();
      if (null == res) return null;
      Map.Entry<K,V> entry = toMapEntry(res);
      cursor.delete();
      return entry;
    }
  }
  
  @Override
  public Map.Entry<K,V> pollLastEntry() {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.last();
      if (null == res) return null;
      Map.Entry<K,V> entry = toMapEntry(res);
      cursor.delete();
      return entry;
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////
  //
  // SortedMap Implementation
  //
  ///////////////////////////////////////////////////////////////////////////   
  
  @Override
  public Comparator<K> comparator() {
    return this;
  }
  
  @Override
  public LMDBSet<Map.Entry<K,V>> entrySet() {
    return entrySet;
  }
  
  @Override
  public K firstKey() {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.first();
      return null != res ? toKey(res) : null;
    }
  }
  
  @Override
  public K lastKey() {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      Cursor cursor = db.openCursor(txn.txn())
    ) {
      KeyAndData res = cursor.last();
      return null != res ? toKey(res) : null;
    }
  }
  
  @Override
  public LMDBValuesCollection<V> values() {
    return values;
  }

  ///////////////////////////////////////////////////////////////////////////
  //
  // Map Implementation
  //
  ///////////////////////////////////////////////////////////////////////////
  
  @Override
  public void clear() {
    try (LMDBTxnInternal txn = withReadWriteTxn()) {
      db.truncateDatabase(txn.txn());
    }
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public boolean containsKey(Object key) {    
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      ReusableBuf cachedKeyBuf = withCachedKeyBuf()
    ) {
      final ByteBuffer buf = db.get(txn.txn(), keySerializer.serialize((K)key, cachedKeyBuf.buf));
      return null != buf;
    }
  }
  
  @Override
  public boolean containsValue(Object value) {
    try (LMDBIterator<Map.Entry<K,V>> it = entrySet().lmdbIterator()) {
      while (it.hasNext()) {
        final Map.Entry<K,V> entry = it.next();
        if (Objects.equals(value, entry.getValue())) return true;
      }
    }
    
    return false;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public V get(Object key) {
    try (
      LMDBTxnInternal txn = withReadOnlyTxn();
      ReusableBuf cachedKeyBuf = withCachedKeyBuf()
    ) {
      final ByteBuffer keyBuf = keySerializer.serialize((K)key, cachedKeyBuf.buf);
      return get(txn.txn(), keyBuf);
    }
  }
  
  V get(Txn txn, ByteBuffer keyBuf) {
    final ByteBuffer buf = db.get(txn, keyBuf);
    return null != buf ? valueSerializer.deserialize(buf) : null;
  }
  
  @Override
  public boolean isEmpty() {
    return 0 == size();
  }
  
  @Override
  public V put(K key, V value) {  
    return put(key, value, true);
  }
  
  @Override
  public boolean add(K key, V value) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      return add(key, keyBuf, value);
    }
  }
  
  @Override
  boolean add(K key, ByteBuffer keyBuf, V value) {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      ReusableBuf cachedValueBuf = withCachedValueBuf()
    ) {
      final ByteBuffer valueBuf = valueSerializer.serialize(value, cachedValueBuf.buf);
      return db.put(txn.txn(), keyBuf, valueBuf, dup ? Api.MDB_NODUPDATA : Api.MDB_NOOVERWRITE);
    }
  }
  
  /**
   * Same as put() but does not return the previous value
   */
  @Override
  public void putNoPrev(K key, V value) {  
    put(key, value, false);
  }
  
  /**
   * Same as put(key, value) but with the option of not looking up and returning the previous value.
   * @param key
   * @param value
   * @param returnPrevious return the previous value?
   * @return The previous value if returnPrevious is true otherwise always null
   */
  V put(K key, V value, boolean returnPrevious) {  
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      ReusableBuf cachedKeyBuf = withCachedKeyBuf();
      ReusableBuf cachedValueBuf = withCachedValueBuf()
    ) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      final ByteBuffer valueBuf = valueSerializer.serialize(value, cachedValueBuf.buf);
      
      V prev = null;
      
      if (returnPrevious) {
        final ByteBuffer prevBuf = db.get(txn.txn(), keyBuf);
        
        // Must deserialize this *before* the db.put command since db.put might
        // overwrite the memory location that prevBuf points to
        prev = null != prevBuf ? valueSerializer.deserialize(prevBuf) : null;
      }
      
      db.put(txn.txn(), keyBuf, valueBuf);
      
      return prev;
    }
  }
  
  @Override
  public boolean prepend(K key, V value) {
    putNoPrev(key, value);
    return true;
  }
  
  @Override
  public boolean append(K key, V value) {
    try (
      LMDBTxnInternal txn = withReadWriteTxn();
      ReusableBuf cachedKeyBuf = withCachedKeyBuf();
      ReusableBuf cachedValueBuf = withCachedValueBuf()
    ) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      final ByteBuffer valueBuf = valueSerializer.serialize(value, cachedValueBuf.buf);

      return db.append(txn.txn(), keyBuf, valueBuf);
    }
  }
  
  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    try (LMDBTxnInternal txn = withReadWriteTxn()) {
      for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
        try (
          ReusableBuf cachedKeyBuf = withCachedKeyBuf();
          ReusableBuf cachedValueBuf = withCachedValueBuf()
        ) {
          db.put(txn.txn(), keySerializer.serialize(entry.getKey(), cachedKeyBuf.buf), valueSerializer.serialize(entry.getValue(), cachedValueBuf.buf));
        }
      }
    }
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public V remove(Object key) {
    try (
     LMDBTxnInternal txn = withReadWriteTxn();
     ReusableBuf cachedKeyBuf = withCachedKeyBuf()
    ) {
      final ByteBuffer keyBuf = keySerializer.serialize((K)key, cachedKeyBuf.buf);
      final ByteBuffer prevBuf = db.get(txn.txn(), keyBuf);
      
      if (null == prevBuf) return null;
      
      // Must deserialize *before* the db.delete call since after
      // delete we are not guaranteed that prevBuf still points to 
      // value data.
      final V prev = valueSerializer.deserialize(prevBuf);
      
      db.delete(txn.txn(), keyBuf);
      return prev;
    }
  }
  
  @Override
  public boolean removeNoPrev(K key) {
    try (ReusableBuf cachedKeyBuf = withCachedKeyBuf()) {
      final ByteBuffer keyBuf = keySerializer.serialize(key, cachedKeyBuf.buf);
      return removeNoPrev(key, keyBuf);
    }
  }
  
  @Override
  boolean removeNoPrev(K key, ByteBuffer keyBuf) {
    try (LMDBTxnInternal txn = withReadWriteTxn()) {
      return db.delete(txn.txn(), keyBuf);
    }
  }
  
  @Override
  public int size() {
    final long size = valueCount();
    if (size > Integer.MAX_VALUE) throw new RuntimeException("size() is too large for a int: "+size);
    return (int)size;
  }
  
  public long keyCount() {
    if (dup) {
      try (
        final LMDBTxnInternal txn = withReadOnlyTxn();
        final Cursor cursor = db.openCursor(txn.txn())
      ) {
        return cursor.keyCount();
      }
    } else {
      return valueCount();
    }
  }
  
  public long valueCount() {
    try (final LMDBTxnInternal txn = withReadOnlyTxn()) {
      final long size = db.stat(txn.txn()).entries;
      return size;
    }
  }
  
  void assertOpen() {
    if (closed.get()) throw new IllegalStateException("Database has been closed!");
  }
  
  void assertWritable() {
    if (env.readOnly()) throw new UnsupportedOperationException("This map is marked as read-only!");
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      db.close();
    }
  }
}
