package eluvio.lmdb.map;

import java.util.Iterator;

/**
 * {@inheritDoc}
 * 
 */
public interface LMDBIterable<T> extends Iterable<T> {
  /**
   * {@inheritDoc}
   * <p>
   * <b>Note: This iterator can only be used within an existing transaction</b>
   */
  public Iterator<T> iterator();
  
  /**
   * Similar to {@link #iterator} but returns a {@link LMDBIterator} that
   * <b>must be closed</b> by the user.  Unlike {@link #iterator} this method
   * does not need to be called within an existing transaction.
   * <p>
   * <b>Note: This iterator <b>MUST BE CLOSED</b> by the user to avoid leaking resources</b>
   */
  public LMDBIterator<T> lmdbIterator();
}
