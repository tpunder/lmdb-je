package eluvio.lmdb.map;

import java.nio.ByteBuffer;

/**
 * Meant to be used by a single thread.  Not Thread Safe.
 */
final class ReusableBuf implements AutoCloseable {
  public final ByteBuffer buf;
  private boolean inUse = false;
  
  public ReusableBuf(ByteBuffer buf) {
    this.buf = buf;
  }
  
  public void open() {
    if (inUse) throw new AssertionError("ReusableBufAlready in use!");
    inUse = true;
  }
  
  public void close() {
    if (null != buf) buf.clear();
    inUse = false;
  }
}