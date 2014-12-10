package eluvio.lmdb.map;

import java.util.Iterator;

/**
 * {@inheritDoc}
 * Note: All Iterators for LMDBMap must be closed
 */
public interface LMDBIterator<T> extends Iterator<T>, AutoCloseable {
  /**
   * Close the underlying LMDB Cursor.  <b>If you fail to call this you will
   * cause a memory leak!</b>
   */
  public void close();
}