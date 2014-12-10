package eluvio.lmdb.map;

import java.nio.ByteBuffer;
import java.util.Map;

interface LMDBCursor<K,V> extends AutoCloseable {
  enum Mode { READ_ONLY, READ_WRITE, USE_EXISTING_TXN }
  
  void close();
  
  boolean moveTo(K key, ByteBuffer keyBuf);
  boolean moveTo(K key, ByteBuffer keyBuf, V value, ByteBuffer valueBuf);
  
  boolean readOnly();
  
  void delete();
  
  Map.Entry<K,V> first();
  Map.Entry<K,V> last();
  Map.Entry<K,V> next();
  Map.Entry<K,V> prev();
  
  K firstKey();
  K lastKey();
  K nextKey();
  K prevKey();
  
  V firstValue();
  V lastValue();
  V nextValue();
  V prevValue();
  
  V firstDupValue();
  V lastDupValue();
  V nextDupValue();
  V prevDupValue();
  
  V dupCeiling(ByteBuffer keyBuf, ByteBuffer valueBuf);
  V dupFloor(ByteBuffer keyBuf, ByteBuffer valueBuf);
  V dupHigher(ByteBuffer keyBuf, ByteBuffer valueBuf);
  V dupLower(ByteBuffer keyBuf, ByteBuffer valueBuf);
  
  long dupCount();
}
