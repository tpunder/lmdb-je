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
 * Note: All Iterators for LMDBMap must be closed
 */
public interface LMDBIterator<T> extends Iterator<T>, AutoCloseable {
  /**
   * Close the underlying LMDB Cursor.  <b>If you fail to call this you will
   * cause a memory leak!</b>
   */
  public void close();
}