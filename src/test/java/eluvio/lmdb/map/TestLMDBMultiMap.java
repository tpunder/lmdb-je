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

import static org.junit.Assert.*;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;

import org.junit.Test;

public class TestLMDBMultiMap extends TestLMDBCommon {
  
  private final int VALUES_PER_KEY = 3;
  
  @Test
  public void basics() {
    try (LMDBMultiMap<String,String> map = new LMDBMultiMapStandalone<String,String>(LMDBSerializer.String, LMDBSerializer.String)) {
      assertEquals(true, map.isEmpty());
      assertEquals(0, map.size());
      
      assertEquals(true, map.keySet().isEmpty());
      assertEquals(true, map.entrySet().isEmpty());
          
      assertArrayEquals(new Object[0], map.keySet().toArray());
      assertArrayEquals(new Object[0], map.values().toArray());
      assertArrayEquals(new Object[0], map.entrySet().toArray());
      
      assertEquals(true, map.add("foo", "bar"));
      assertEquals(1, map.keyCount());
      assertEquals(1, map.valueCount());
      assertEquals(1, map.size());      
      assertArrayEquals(new String[]{ "bar" }, map.get("foo").toArray());
      
      assertEquals(true, map.add("foo", "baz"));
      assertEquals(1, map.keyCount());
      assertEquals(2, map.valueCount());
      assertEquals(2, map.size());
      assertArrayEquals(new String[]{ "bar", "baz" }, map.get("foo").toArray());
      
      
      assertEquals(false, map.isEmpty());
      assertEquals(false, map.keySet().isEmpty());
      assertEquals(false, map.entrySet().isEmpty());
      
      assertEquals(true, map.add("bar", "bar2"));
      assertEquals(2, map.keyCount());
      assertEquals(3, map.valueCount());
      assertEquals(3, map.size());
      
      assertEquals(true, map.add("zzz", "zzz2"));
      assertEquals(3, map.keyCount());
      assertEquals(4, map.valueCount());
      assertEquals(4, map.size());
      
      assertEquals(true, map.add("zzz", "zzz_dup"));
      assertEquals(3, map.keyCount());
      assertEquals(5, map.valueCount());
      assertEquals(5, map.size());
      assertArrayEquals(new String[]{ "zzz2", "zzz_dup" }, map.get("zzz").toArray());
      
      assertEquals("bar", map.firstKey());
      assertEquals("zzz", map.lastKey());
      
      assertArrayEquals(new String[]{ "bar", "foo", "zzz" }, map.keySet().toArray());
      assertArrayEquals(new String[]{ "bar2", "bar", "baz", "zzz2", "zzz_dup" }, map.values().toArray());
      
      assertEquals(false, map.isEmpty());
      
      assertEquals(false, map.removeAll("does_not_exist"));
      assertEquals(false, map.remove("key_does_not_exist", "value_does_not_exist"));
      assertEquals(3, map.keyCount());
      assertEquals(5, map.valueCount());
      assertEquals(5, map.size());
      
      assertEquals(false, map.remove("foo", "value_does_not_exist"));
      assertEquals(3, map.keyCount());
      assertEquals(5, map.valueCount());
      assertEquals(5, map.size());
      
      assertEquals(true, map.remove("foo", "baz"));
      assertArrayEquals(new String[]{ "bar" }, map.get("foo").toArray());
      assertEquals(3, map.keyCount());
      assertEquals(4, map.valueCount());
      assertEquals(4, map.size());
      
      assertEquals(true, map.remove("foo", "bar"));
      assertEquals(true, map.get("foo").isEmpty());
      assertArrayEquals(new String[]{ }, map.get("foo").toArray());
      
      assertEquals(true, map.removeAll("zzz"));
      assertEquals(true, map.get("zzz").isEmpty());
      assertArrayEquals(new String[]{ }, map.get("zzz").toArray());
      
      assertEquals(1, map.keyCount());
      assertEquals(1, map.valueCount());
      assertEquals(1, map.size());
      
      map.clear();
      
      assertEquals(0, map.keyCount());
      assertEquals(0, map.valueCount());
      assertEquals(0, map.size());
      assertTrue(map.isEmpty());
    }
  }
  
  @Test
  public void testMultiSet() {
    try (LMDBMultiMap<Long,Long> map = makeLongLongMap()) {
      final LMDBMultiSet<Long> set = map.get(123L);
      set.clear();
      assertTrue(set.isEmpty());
      assertEquals(0, set.size());
      
      assertNull(set.first());
      assertNull(set.last());
      assertNull(set.ceiling(123L));
      assertNull(set.floor(123L));
      assertNull(set.higher(123L));
      assertNull(set.lower(123L));
      
      final Long[] vals = new Long[]{Long.MIN_VALUE+100L, -1000L, -100L, -10L, 0L, 10L, 100L, 1000L, Long.MAX_VALUE-100L};
      
      set.addAll(Arrays.asList(vals));
      
      assertEquals(Long.valueOf(Long.MIN_VALUE+100L), set.ceiling(Long.MIN_VALUE));
      assertEquals(Long.valueOf(Long.MIN_VALUE+100L), set.higher(Long.MIN_VALUE));
      assertEquals(null, set.floor(Long.MIN_VALUE));
      assertEquals(null, set.lower(Long.MIN_VALUE));
      
      assertEquals(null, set.ceiling(Long.MAX_VALUE));
      assertEquals(null, set.higher(Long.MAX_VALUE));
      assertEquals(Long.valueOf(Long.MAX_VALUE-100L), set.floor(Long.MAX_VALUE));
      assertEquals(Long.valueOf(Long.MAX_VALUE-100L), set.lower(Long.MAX_VALUE));
      
      assertEquals(Long.valueOf(100L), set.ceiling(15L));
      assertEquals(Long.valueOf(100L), set.higher(15L));
      assertEquals(Long.valueOf(10L), set.floor(15L));
      assertEquals(Long.valueOf(10L), set.lower(15L));
      
      assertEquals(Long.valueOf(-10L), set.ceiling(-15L));
      assertEquals(Long.valueOf(-10L), set.higher(-15L));
      assertEquals(Long.valueOf(-100L), set.floor(-15L));
      assertEquals(Long.valueOf(-100L), set.lower(-15L));
      
      checkMultiSet(set, vals);

      checkMultiSet(set.headSet(-5L), new Long[]{Long.MIN_VALUE+100L, -1000L, -100L, -10L});
      checkMultiSet(set.tailSet(5L), new Long[]{10L, 100L, 1000L, Long.MAX_VALUE-100L});
      checkMultiSet(set.subSet(-15L, 15L), new Long[]{-10L, 0L, 10L});
      
      checkMultiSet(set.headSet(-10L), new Long[]{Long.MIN_VALUE+100L, -1000L, -100L});
      checkMultiSet(set.headSet(-10L, false), new Long[]{Long.MIN_VALUE+100L, -1000L, -100L});
      checkMultiSet(set.headSet(-10L, true), new Long[]{Long.MIN_VALUE+100L, -1000L, -100L, -10L});
      
      checkMultiSet(set.tailSet(10L), new Long[]{10L, 100L, 1000L, Long.MAX_VALUE-100L});
      checkMultiSet(set.tailSet(10L, true), new Long[]{10L, 100L, 1000L, Long.MAX_VALUE-100L});
      checkMultiSet(set.tailSet(10L, false), new Long[]{100L, 1000L, Long.MAX_VALUE-100L});
      
      checkMultiSet(set.subSet(-100L, 100L), new Long[]{-100L, -10L, 0L, 10L});
      checkMultiSet(set.subSet(-100L, true, 100L, false), new Long[]{-100L, -10L, 0L, 10L});
      checkMultiSet(set.subSet(-100L, true, 100L, true), new Long[]{-100L, -10L, 0L, 10L, 100L});
      checkMultiSet(set.subSet(-100L, false, 100L, true), new Long[]{-10L, 0L, 10L, 100L});
    }
  }
  
  private LMDBMultiMap <Long,Long> makeLongLongMap() {
    final LMDBMultiMapStandalone<Long,Long> map = new LMDBMultiMapStandalone<Long,Long>(LMDBSerializer.Long, LMDBSerializer.Long);
    
    map.add(Long.MIN_VALUE, Long.MIN_VALUE);
    map.add(Long.MIN_VALUE, Long.MIN_VALUE+1);
    map.add(Long.MIN_VALUE, Long.MIN_VALUE+2);
    
    map.add(Long.MAX_VALUE, Long.MAX_VALUE);
    map.add(Long.MAX_VALUE, Long.MAX_VALUE-1);
    map.add(Long.MAX_VALUE, Long.MAX_VALUE-2);
    
    try (LMDBTxn txn = map.withReadWriteTxn()) {
      for (long i = -10_000L; i <= 10_000L; i++) {
        map.add(i, i);
        map.add(i, i-1);
        map.add(i, i+1);
      }
    }
    
    return map;
  }
  
  private Long[] longMapKeys() {
    return concat(Long.valueOf(Long.MIN_VALUE), range(-10_000L, true, 10_000L, true), Long.valueOf(Long.MAX_VALUE));
  }
  
  @Test
  public void navigableMap() {
    try (LMDBMultiMap<Long,Long> map = makeLongLongMap()) {
      checkContents(map, longMapKeys());
      
      assertEquals(20003, map.keyCount());
      assertEquals(20003 * VALUES_PER_KEY, map.valueCount());
      assertEquals(20003 * VALUES_PER_KEY, map.size());
      
      first(map, Long.MIN_VALUE);
      last(map, Long.MAX_VALUE);
      
      ceiling(map, 0L, 0L);
      higher(map, 1L, 0L);
      floor(map, 0L, 0L);
      lower(map, -1L, 0L);
      
      ceiling(map, 1000L, 1000L);
      higher(map, 1001L, 1000L);
      floor(map, 1000L, 1000L);
      lower(map, 999L, 1000L);
      
      ceiling(map, -1000L, -1000L);
      higher(map, -999L, -1000L);
      floor(map, -1000L, -1000L);
      lower(map, -1001L, -1000L);
      
      ceiling(map, -10_000L, -32_768L);
      higher(map, -10_000L, -32_768L);
      floor(map, Long.MIN_VALUE, -32_768L);
      lower(map, Long.MIN_VALUE, -32_768L);
      
      ceiling(map, Long.MAX_VALUE, 32_768L);
      higher(map, Long.MAX_VALUE, 32_768L);
      floor(map, 10_000L, 32_768L);
      lower(map, 10_000L, 32_768L);
      
      checkContents(map.subMap(0L, 10L              ), 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
      checkContents(map.subMap(0L, true,  10L, false), 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
      checkContents(map.subMap(0L, true,  10L, true ), 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
      checkContents(map.subMap(0L, false, 10L, true ), 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
      checkContents(map.subMap(0L, false, 10L, false), 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
      
      checkContents(map.headMap(-9_900L), concat(Long.valueOf(Long.MIN_VALUE), range(-10_000L, -9_900L)));
      checkContents(map.headMap(-9_900L, false), concat(Long.valueOf(Long.MIN_VALUE), range(-10_000L, -9_900L)));
      checkContents(map.headMap(-9_900L, true), concat(Long.valueOf(Long.MIN_VALUE), range(-10_000L, true, -9_900L, true)));

      checkContents(map.tailMap(9_900L), concat(range(9_900L, true, 10_000L, true), Long.valueOf(Long.MAX_VALUE)));
      checkContents(map.tailMap(9_900L, true), concat(range(9_900L, true, 10_000L, true), Long.valueOf(Long.MAX_VALUE)));
      checkContents(map.tailMap(9_900L, false), concat(range(9_900L, false, 10_000L, true), Long.valueOf(Long.MAX_VALUE)));
    }
  }
  
  @Test
  public void reversedNavigableMap() {
    try (LMDBMultiMap<Long,Long> map = makeLongLongMap().descendingMap()) {
      assertEquals(20003, map.keyCount());
      assertEquals(20003 * VALUES_PER_KEY, map.valueCount());
      assertEquals(20003 * VALUES_PER_KEY, map.size());
      
      first(map, Long.MAX_VALUE);
      last(map, Long.MIN_VALUE);
      
      ceiling(map, 0L, 0L);
      higher(map, -1L, 0L);
      floor(map, 0L, 0L);
      lower(map, 1L, 0L);
      
      ceiling(map, 1000L, 1000L);
      higher(map, 999L, 1000L);
      floor(map, 1000L, 1000L);
      lower(map, 1001L, 1000L);
      
      ceiling(map, -1000L, -1000L);
      higher(map, -1001L, -1000L);
      floor(map, -1000L, -1000L);
      lower(map, -999L, -1000L);
      
      ceiling(map, Long.MIN_VALUE, -32_768L);
      higher(map, Long.MIN_VALUE, -32_768L);
      floor(map, -10_000L, -32_768L);
      lower(map, -10_000L, -32_768L);

      ceiling(map, 10_000L, 32_768L);
      higher(map, 10_000L, 32_768L);
      floor(map, Long.MAX_VALUE, 32_768L);
      lower(map, Long.MAX_VALUE, 32_768L);
      
      checkContents(map.subMap(10L, 0L              ), 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
      checkContents(map.subMap(10L, true,  0L, false), 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
      checkContents(map.subMap(10L, true,  0L, true ), 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L);
      checkContents(map.subMap(10L, false, 0L, true ), 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L);
      checkContents(map.subMap(10L, false, 0L, false), 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
      
      checkContents(map.headMap(9_900L), concat(Long.valueOf(Long.MAX_VALUE), range(10_000L, true, 9_900L, true)));
      checkContents(map.headMap(9_900L, true), concat(Long.valueOf(Long.MAX_VALUE), range(10_000L, true, 9_900L, true)));
      checkContents(map.headMap(9_900L, false), concat(Long.valueOf(Long.MAX_VALUE), range(10_000L, true, 9_900L, false)));

      checkContents(map.tailMap(-9_900L), concat(range(-9_900L, false, -10_000L, true), Long.valueOf(Long.MIN_VALUE)));
      checkContents(map.tailMap(-9_900L, false), concat(range(-9_900L, false, -10_000L, true), Long.valueOf(Long.MIN_VALUE)));
      checkContents(map.tailMap(-9_900L, true), concat(range(-9_900L, true, -10_000L, true), Long.valueOf(Long.MIN_VALUE)));
    }
  }
  
  @Test
  public void headMap() {
    try (LMDBMultiMap<Long,Long> map = makeLongLongMap().headMap(1000L)) {
      assertEquals(11001, map.keyCount());
      assertEquals(11001 * VALUES_PER_KEY, map.valueCount());
      assertEquals(11001 * VALUES_PER_KEY, map.size());
      
      first(map, Long.MIN_VALUE);
      last(map, 999L);
      
      ceiling(map, 0L, 0L);
      higher(map, 1L, 0L);
      floor(map, 0L, 0L);
      lower(map, -1L, 0L);
      
      ceiling(map, null, 1000L);
      higher(map, null, 1000L);
      
      floor(map, 999L, 1000L);
      lower(map, 999L, 1000L);

      ceiling(map, -1000L, -1000L);
      higher(map, -999L, -1000L);
      floor(map, -1000L, -1000L);
      lower(map, -1001L, -1000L);
      
      assertTrue(map.removeAll(0L));
      
      ceiling(map, 1L, 0L);
      higher(map, 1L, 0L);
      floor(map, -1L, 0L);
      lower(map, -1L, 0L);

      assertTrue(map.add(-1_000_000L, -123L));
      try { map.add(1_000_000L, 123L); fail(); } catch (LMDBOutOfRangeException ex) { /* good */ }
    }
  }
  
  @Test
  public void tailMap() {
    try (LMDBMultiMap<Long,Long> map = makeLongLongMap().tailMap(-1000L)) {
      assertEquals(11002, map.keyCount());
      assertEquals(11002 * VALUES_PER_KEY, map.valueCount());
      assertEquals(11002 * VALUES_PER_KEY, map.size());
      
      first(map, -1000L);
      last(map, Long.MAX_VALUE);
      
      ceiling(map, 0L, 0L);
      higher(map, 1L, 0L);
      floor(map, 0L, 0L);
      lower(map, -1L, 0L);
      
      ceiling(map, 1000L, 1000L);
      higher(map, 1001L, 1000L);
      floor(map, 1000L, 1000L);
      lower(map, 999L, 1000L);
      
      ceiling(map, -1000L, -1000L);
      higher(map, -999L, -1000L);
      floor(map, -1000L, -1000L);
      lower(map, null, -1000L);
      
      assertTrue(map.removeAll(0L));
      
      ceiling(map, 1L, 0L);
      higher(map, 1L, 0L);
      floor(map, -1L, 0L);
      lower(map, -1L, 0L);
      
      try { map.add(-1_000_000L, -123L); fail(); } catch (LMDBOutOfRangeException ex) { /* good */ }
      assertTrue(map.add(1_000_000L, 123L));
    }
  }
  
  @Test
  public void subMap() {
    try (LMDBMultiMap<Long,Long> map = makeLongLongMap().subMap(-1000L, 1000L)) {
      assertEquals(2000, map.keyCount());
      assertEquals(2000 * VALUES_PER_KEY, map.valueCount());
      assertEquals(2000 * VALUES_PER_KEY, map.size());
      
      first(map, -1000L);
      last(map, 999L);
      
      ceiling(map, 0L, 0L);
      higher(map, 1L, 0L);
      floor(map, 0L, 0L);
      lower(map, -1L, 0L);
      
      ceiling(map, null, 1000L);
      higher(map, null, 1000L);
      floor(map, 999L, 1000L);
      lower(map, 999L, 1000L);
      
      ceiling(map, -1000L, -1000L);
      higher(map, -999L, -1000L);
      floor(map, -1000L, -1000L);
      lower(map, null, -1000L);
      
      assertTrue(map.removeAll(0L));
      
      ceiling(map, 1L, 0L);
      higher(map, 1L, 0L);
      floor(map, -1L, 0L);
      lower(map, -1L, 0L);
      
      try { map.add(-1_000_000L, -123L); fail(); } catch (LMDBOutOfRangeException ex) { /* good */ }
      try { map.add(1_000_000L, 123L); fail(); } catch (LMDBOutOfRangeException ex) { /* good */ }
    }
  }

  
  private void first(LMDBMultiMap<Long,Long> map, Long expected) {
    assertEquals(expected, map.firstKey());
//    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.firstEntry());
  }
  
  private void last(LMDBMultiMap<Long,Long> map, Long expected) {
    assertEquals(expected, map.lastKey());
//    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.lastEntry());
  }
  
  private void ceiling(LMDBMultiMap<Long,Long> map, Long expected, Long arg) {
    assertEquals(expected, map.ceilingKey(arg));
//    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.ceilingEntry(arg));
  }
  
  private void floor(LMDBMultiMap<Long,Long> map, Long expected, Long arg) {
    assertEquals(expected, map.floorKey(arg));
//    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.floorEntry(arg));
  }
  
  private void higher(LMDBMultiMap<Long,Long> map, Long expected, Long arg) {
    assertEquals(expected, map.higherKey(arg));
//    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.higherEntry(arg));
  }
  
  private void lower(LMDBMultiMap<Long,Long> map, Long expected, Long arg) {
    assertEquals(expected, map.lowerKey(arg));
//    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.lowerEntry(arg));
  }
  
  private void checkContents(LMDBMultiMap<Long,Long> map, Long... keys) {
    checkContentsImpl(map, keys);
    checkContentsImpl(map.descendingMap(), reverse(keys));
//    assertSame(map, map.descendingMap().descendingMap());
  }
  
  @SuppressWarnings("unchecked")
  private void checkContentsImpl(LMDBMultiMap<Long,Long> map, Long... keys) {
    final Long[] vals = new Long[keys.length * VALUES_PER_KEY];
    final SimpleImmutableEntry<Long,Long>[] entries = new SimpleImmutableEntry[keys.length * VALUES_PER_KEY];
    
    final int off = keys[0] < keys[1] ? 1 : -1;
    
    for (int i = 0; i < keys.length; i++) {
      final long key = keys[i];
      long num = keys[i];
      final int entryIdx = i * VALUES_PER_KEY;
      
      if (num == Long.MAX_VALUE) num--;
      if (num == Long.MIN_VALUE) num++;
      
      vals[entryIdx] = num-off;
      vals[entryIdx+1] = num;
      vals[entryIdx+2] = num+off;
      
      entries[entryIdx] = new SimpleImmutableEntry<Long,Long>(key, num-off);
      entries[entryIdx+1] = new SimpleImmutableEntry<Long,Long>(key, num);
      entries[entryIdx+2] = new SimpleImmutableEntry<Long,Long>(key, num+off);
    }

    assertEquals(keys.length, map.keyCount());
    assertEquals(keys.length * VALUES_PER_KEY, map.valueCount());
    assertEquals(keys.length * VALUES_PER_KEY, map.size());
    
    checkKeySet(keys, map.keySet());
    checkCollection(entries, map.entrySet());
    checkCollection(vals, map.values());
    
    checkKeySet(reverse(keys), map.keySet().descendingSet());
    
    for (int i = 0; i < keys.length; i++) {
      ceiling(map, keys[i], keys[i]);
      floor(map, keys[i], keys[i]);
      lower(map, 0 == i ? null : keys[i-1], keys[i]);
      higher(map, keys.length - 1 == i ? null : keys[i+1], keys[i]);
      
      final int entryIdx = i * VALUES_PER_KEY;
      
      checkMultiSet(map.get(keys[i]), vals[entryIdx], vals[entryIdx+1], vals[entryIdx+2]);
    }
  }
  
  @SuppressWarnings("unchecked")
  private <V> void checkMultiSet(LMDBMultiSet<V> set, V... values) {
    checkMultiSetImpl(set, values);
    checkMultiSetImpl(set.descendingSet(), reverse(values));
  }
  
  @SuppressWarnings("unchecked")
  private <V> void checkMultiSetImpl(LMDBMultiSet<V> set, V... values) {
    assertEquals(values.length, set.size());
    assertEquals(values[0], set.first());
    assertEquals(values[values.length - 1], set.last());
    
    checkCollection(values, set);
    
    assertTrue(set.containsAll(set));
    
    // TODO: add subMap/headMap/tailMap testing here
    
    for (int i = 0; i < values.length; i++) {
      assertTrue(set.contains(values[i]));
      assertEquals(values[i], set.ceiling(values[i]));
      assertEquals(values[i], set.floor(values[i]));
      assertEquals(0 == i ? null : values[i-1], set.lower(values[i]));
      assertEquals(values.length - 1 == i ? null : values[i+1], set.higher(values[i]));
    }
  }
}
