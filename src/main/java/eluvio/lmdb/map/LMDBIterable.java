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
