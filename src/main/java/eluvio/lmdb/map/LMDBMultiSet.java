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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * A set containing all the duplicate values for a key in an
 * {@link LMDBMultiMap} instance.
 * <p>
 * Changes made to this set are reflecting in the
 * {@link LMDBMultiMap} and vice versa.
 * <p>
 * Implementations of this interface are <b>thread-safe</b>.
 * 
 * @param <V> the element type
 */
public interface LMDBMultiSet<V> extends LMDBSet<V>, NavigableSet<V>, Comparator<V> {
  @Override
  Iterator<V> descendingIterator();
  
  LMDBIterator<V> descendingLMDBIterator();

  @Override
  LMDBMultiSet<V> descendingSet();

  @Override
  LMDBMultiSet<V> headSet(V toElement);

  @Override
  LMDBMultiSet<V> headSet(V toElement, boolean inclusive);

  @Override
  LMDBMultiSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive);

  @Override
  LMDBMultiSet<V> subSet(V fromElement, V toElement);

  @Override
  LMDBMultiSet<V> tailSet(V fromElement);

  @Override
  LMDBMultiSet<V> tailSet(V fromElement, boolean inclusive);
}
