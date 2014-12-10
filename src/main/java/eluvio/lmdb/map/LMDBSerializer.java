package eluvio.lmdb.map;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public abstract class LMDBSerializer<T> {
  /**
   * 
   * @return The size of the cached buffer to keep around per-thread if greater than or equal to 0
   */
  public abstract int cachedBufferSize();
  
  /**
   * Corresponds to the {@link eluvio.lmdb.api.Api#MDB_INTEGERKEY} flag (only applicable to keys)
   * @return true if you want the MDB_INTEGERKEY flag set
   */
  public abstract boolean integerKeys();
  
  /**
   * Serialize the object
   * @param data The object to serialize
   * @param buf An optional ByteBuffer that can be passed in that can be used
   *        if it has enough space.  (e.g. Since keys have a 511 byte size
   *        limit) we can create a direct ByteBuffer of size 511 and just
   *        reuse it.  NOTE: This parameter might be null.
   * @return The *direct* ByteBuffer that contains the serialized data (be
   *         sure to flip() it.  If you used a non-null ByteBuffer that was
   *         passed in then you should return that ByteBuffer)
   */
  public abstract ByteBuffer serialize(T data, ByteBuffer buf);
  
  /**
   * Deserialize data from a ByteBuffer
   * @param buf The ByteBuffer that contains the data.  Note: DO NOT WRITE TO
   *        THIS BUFFER since it probably points directly to the data in the
   *        database.
   * @return The deserialized object
   */
  public abstract T deserialize(ByteBuffer buf);
  
  /**
   * A string serializer for UTF-8 Strings
   */
  public final static LMDBSerializer<String> String = new LMDBSerializer<String>() {
    public int cachedBufferSize() { return -1; }
    public boolean integerKeys() { return false; }

//    public ByteBuffer serialize(String s, ByteBuffer buf) {
//      return UTF8.write(s, buf);
//    }
//    
//    public String deserialize(ByteBuffer buf) {
//      return UTF8.read(buf);
//    }
//    
    // NOTE - Can't just do buf.putChar(s.charAt(i)) because it writes 2-bytes per char
    public ByteBuffer serialize(String s, ByteBuffer buf) {
      final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      
      if (null == buf || bytes.length > buf.remaining()) buf = ByteBuffer.allocateDirect(bytes.length);
      
      buf.put(bytes);
      buf.flip();
      return buf;
    }
    
    public String deserialize(ByteBuffer buf) {
      final byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  };
  
  /**
   * A serializer for <b>signed</b> integers.
   * <p>
   * Note: This uses LMDB's {@link eluvio.lmdb.api.Api#MDB_INTEGERKEY} flag which only works with unsigned
   * integers so this serializer converts the signed integer to an unsigned int
   * by subtracting {@link Integer#MIN_VALUE} from the signed int.
   */
  public final static LMDBSerializer<Integer> Int = new IntSerializerBase() {
    protected int write(int i){ return i - Integer.MIN_VALUE; }
    protected int read(int i){ return i + Integer.MIN_VALUE; }
  };
  
  /**
   * A serializer for <b>unsigned</b> integers.
   * <p>
   * This treats ints as unsigned and performs no conversion on them.  This
   * means that {@link Integer#MIN_VALUE} is greater than {@link Integer#MAX_VALUE}
   */
  public final static LMDBSerializer<Integer> UnsignedInt = new IntSerializerBase() {
    protected int write(int i){ return i; }
    protected int read(int i){ return i; }
  };
  
  private static abstract class IntSerializerBase extends LMDBSerializer<Integer> {
    private final int size = 4;
    
    public int cachedBufferSize() { return size; }
    public boolean integerKeys() { return true; }

    protected abstract int write(int i);
    protected abstract int read(int i);
    
    public ByteBuffer serialize(Integer i, ByteBuffer buf) {
      if (null == buf || buf.remaining() < size) buf = ByteBuffer.allocateDirect(size);
      setByteOrder(buf);
      buf.putInt(write(i));
      buf.flip();
      return buf;
    }
    
    public Integer deserialize(ByteBuffer buf) {
      setByteOrder(buf);
      return read(buf.getInt());
    }
    
    private void setByteOrder(ByteBuffer buf) {
      // Native ByteOrder is needed for MDB_INTEGERKEY to work
      if (buf.order() != ByteOrder.nativeOrder()) buf.order(ByteOrder.nativeOrder());
    }
  };
  
  /**
   * A serializer for <b>signed</b> longs.
   * <p>
   * Note: This uses LMDB's {@link eluvio.lmdb.api.Api#MDB_INTEGERKEY} flag which only works with unsigned
   * numbers so this serializer converts the signed long to an unsigned long
   * by subtracting {@link Long#MIN_VALUE} from the signed long.
   */
  public final static LMDBSerializer<Long> Long = new LongSerializerBase() {
    protected long write(long i){ return i - java.lang.Long.MIN_VALUE; }
    protected long read(long i){ return i + java.lang.Long.MIN_VALUE; }
  };
  
  /**
   * A serializer for <b>unsigned</b> longs.
   * <p>
   * This treats longs as unsigned and performs no conversion on them.  This
   * means that {@link Integer#MIN_VALUE} is greater than {@link Integer#MAX_VALUE}
   */
  public final static LMDBSerializer<Long> UnsignedLong = new LongSerializerBase() {
    protected long write(long i){ return i; }
    protected long read(long i){ return i; }
  };

  private static abstract class LongSerializerBase extends LMDBSerializer<Long> {
    private final int size = 8;
    
    public int cachedBufferSize() { return size; }
    public boolean integerKeys() { return true; }

    protected abstract long write(long i);
    protected abstract long read(long i);
    
    public ByteBuffer serialize(Long i, ByteBuffer buf) {
      if (null == buf || buf.remaining() < size) buf = ByteBuffer.allocateDirect(size);
      setByteOrder(buf);
      buf.putLong(write(i));
      buf.flip();
      return buf;
    }
    
    public Long deserialize(ByteBuffer buf) {
      setByteOrder(buf);
      return read(buf.getLong());
    }
    
    private void setByteOrder(ByteBuffer buf) {
      // Native ByteOrder is needed for MDB_INTEGERKEY to work
      if (buf.order() != ByteOrder.nativeOrder()) buf.order(ByteOrder.nativeOrder());
    }
  };
  
  /**
   * A simple serializer for byte arrays
   */
  public final static LMDBSerializer<byte[]> ByteArray = new LMDBSerializer<byte[]>() {
    public int cachedBufferSize() { return -1; }
    public boolean integerKeys() { return false; }

    public ByteBuffer serialize(byte[] b, ByteBuffer buf) {
      if (null == buf || buf.remaining() < b.length) buf = ByteBuffer.allocateDirect(b.length);
      buf.put(b);
      buf.flip();
      return buf;
    }
    
    public byte[] deserialize(ByteBuffer buf) {
      final byte[] b = new byte[buf.remaining()];
      buf.get(b);
      return b;
    }
  };
}