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

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;

import org.junit.Test;

public class TestLMDBMap extends TestLMDBCommon {

  interface Foo extends Serializable {
    String tpe();
  }

  final static class Bar implements Foo {
    public String bar = "bar";
    public String tpe() { return "bar"; }
  }

  final static class Baz implements Foo {
    public String baz = "baz";
    public String tpe() { return "baz"; }
  }

  @Test
  public void objectSerialization() {
    try (LMDBMapStandalone<String,Foo> map = new LMDBMapStandalone<String,Foo>(LMDBSerializer.String, new LMDBSerializer.ObjectSerializer<Foo>())) {
      map.put("bar", new Bar());
      map.put("baz", new Baz());

      assertEquals(map.get("bar").tpe(), "bar");
      assertEquals(map.get("baz").tpe(), "baz");

      assertEquals(map.get("bar").getClass(), Bar.class);
      assertEquals(map.get("baz").getClass(), Baz.class);
    }
  }

  @Test
  public void empty() {
    try (LMDBMapStandalone<String,String> map = new LMDBMapStandalone<String,String>(LMDBSerializer.String, LMDBSerializer.String)) {
      assertTrue(map.isEmpty());
      assertEquals(0, map.keyCount());
      assertEquals(0, map.valueCount());
      assertEquals(0, map.size());
      
      assertNull(map.get("123"));
      assertFalse(map.contains("123", "123"));
      assertFalse(map.containsKey("123"));
      assertFalse(map.containsValue("123"));
      
      assertNull(map.firstKey());
      assertNull(map.lastKey());
      
      assertNull(map.firstEntry());
      assertNull(map.lastEntry());
      
      assertNull(map.ceilingKey("123"));
      assertNull(map.floorKey("123"));
      assertNull(map.higherKey("123"));
      assertNull(map.lowerKey("123"));
      
      assertNull(map.ceilingEntry("123"));
      assertNull(map.floorEntry("123"));
      assertNull(map.higherEntry("123"));
      assertNull(map.lowerEntry("123"));
      
      assertTrue(map.entrySet().isEmpty());
      assertTrue(map.keySet().isEmpty());
      assertTrue(map.values().isEmpty());
      
      try (LMDBIterator<Map.Entry<String,String>> it = map.entrySet().lmdbIterator()) {
        assertFalse(it.hasNext());
      }
      
      try (LMDBIterator<String> it = map.keySet().lmdbIterator()) {
        assertFalse(it.hasNext());
      }
      
      try (LMDBIterator<String> it = map.values().lmdbIterator()) {
        assertFalse(it.hasNext());
      }
    }
  }
  
  @Test
  public void basics() {
    try (LMDBMapStandalone<String,String> map = new LMDBMapStandalone<String,String>(LMDBSerializer.String, LMDBSerializer.String)) {
      assertEquals(true, map.isEmpty());
      assertEquals(0, map.size());
      
      assertEquals(true, map.keySet().isEmpty());
      assertEquals(true, map.entrySet().isEmpty());
          
      assertArrayEquals(new Object[0], map.keySet().toArray());
      assertArrayEquals(new Object[0], map.values().toArray());
      assertArrayEquals(new Object[0], map.entrySet().toArray());
      
      assertEquals(null, map.put("foo", "bar"));
      assertEquals(1, map.size());
      assertEquals("bar", map.get("foo"));
      assertEquals("bar", map.put("foo", "baz"));
      assertEquals("baz", map.get("foo"));
      assertEquals(1, map.size());
      
      assertEquals(false, map.isEmpty());
      assertEquals(false, map.keySet().isEmpty());
      assertEquals(false, map.entrySet().isEmpty());
      
      assertEquals(null, map.put("bar", "bar2"));
      assertEquals(2, map.size());
      
      assertEquals(null, map.put("zzz", "zzz2"));
      assertEquals(3, map.size());
      
      assertEquals(false, map.add("zzz", "should_not_be_added"));
      assertEquals("zzz2", map.get("zzz"));
      
      assertEquals("bar", map.firstKey());
      assertEquals("zzz", map.lastKey());
      
      assertArrayEquals(new String[]{ "bar", "foo", "zzz" }, map.keySet().toArray());
      assertArrayEquals(new String[]{ "bar2", "baz", "zzz2" }, map.values().toArray());
      
      assertEquals(false, map.isEmpty());
      
      assertEquals(null, map.remove("does_not_exist"));
      assertEquals(3, map.size());
      
      assertEquals(false, map.removeNoPrev("does_not_exist_2"));
      assertEquals(3, map.size());
      
      assertEquals(false, map.remove("foo", "wrong_value"));
      assertEquals(3, map.size());
      assertEquals("baz", map.get("foo"));
      
      assertEquals(true, map.remove("foo", "baz"));
      assertEquals(null, map.get("foo"));
      assertEquals(2, map.size());
      
      assertEquals(true, map.removeNoPrev("zzz"));
      assertEquals(1, map.size());
      
      map.clear();
      
      assertEquals(0, map.keyCount());
      assertEquals(0, map.valueCount());
      assertEquals(0, map.size());
      assertTrue(map.isEmpty());
    }
  }
  
  @Test
  public void removeReplace() {
    try (LMDBMapStandalone<String,String> map = new LMDBMapStandalone<String,String>(LMDBSerializer.String, LMDBSerializer.String)) {
      map.putNoPrev("foo", "bar");
      
      assertTrue(map.contains("foo", "bar"));
      assertTrue(map.containsKey("foo"));
      assertTrue(map.containsValue("bar"));
      
      assertFalse(map.contains("foo", "nope"));
      assertFalse(map.containsKey("zzz"));
      assertFalse(map.containsValue("nope"));
      
      assertEquals("bar", map.get("foo"));
      
      assertFalse(map.remove("foo", "asd"));
      assertEquals("bar", map.get("foo"));
      
      assertTrue(map.remove("foo", "bar"));
      assertEquals(null, map.get("foo"));
      
      assertEquals(null, map.putIfAbsent("foo", "val"));
      assertEquals("val", map.putIfAbsent("foo", "nope"));
      assertEquals("val", map.get("foo"));
      
      assertEquals("val", map.replace("foo", "new_val"));
      assertEquals("new_val", map.get("foo"));
      
      assertFalse(map.replace("foo", "nope", "another_val"));
      assertEquals("new_val", map.get("foo"));
      
      assertTrue(map.replace("foo", "new_val", "another_val"));
      assertEquals("another_val", map.get("foo"));
      
      assertEquals(null, map.replace("bar", "bar_val"));
      assertEquals(null, map.get("bar"));
      
      assertEquals(null, map.put("bar", "bar_val"));
      assertEquals("bar_val", map.get("bar"));
      
      assertNull(map.remove("does_not_exist"));
      assertEquals("bar_val", map.remove("bar"));
    }
  }
  
  @Test
  public void poll() {
    try (LMDBMapStandalone<String,String> map = new LMDBMapStandalone<String,String>(LMDBSerializer.String, LMDBSerializer.String)) {
      assertTrue(map.append("aaa", "one"));
      assertTrue(map.append("bbb", "two"));
      assertTrue(map.append("ccc", "three"));
      assertTrue(map.append("ddd", "four"));
      assertTrue(map.append("eee", "five"));
      assertTrue(map.append("fff", "six"));
      assertTrue(map.append("ggg", "seven"));
      assertTrue(map.append("hhh", "eight"));
      assertTrue(map.append("iii", "nine"));
      assertTrue(map.append("jjj", "ten"));
      
      assertFalse(map.append("abc", "nope"));
      
      assertEquals("aaa", map.pollFirstKey());
      assertEquals("bbb", map.pollFirstKey());
      
      assertEquals("jjj", map.pollLastKey());
      assertEquals("iii", map.pollLastKey());
      
      assertEquals(new AbstractMap.SimpleImmutableEntry<String,String>("ccc", "three"), map.pollFirstEntry());
      assertEquals(new AbstractMap.SimpleImmutableEntry<String,String>("ddd", "four"), map.pollFirstEntry());
      
      assertEquals(new AbstractMap.SimpleImmutableEntry<String,String>("hhh", "eight"), map.pollLastEntry());
      assertEquals(new AbstractMap.SimpleImmutableEntry<String,String>("ggg", "seven"), map.pollLastEntry());
      
      assertEquals(2, map.size());
    }
  }
  
  private LMDBMapStandalone <Long,String> makeLongStringMap() {
    final LMDBMapStandalone<Long,String> map = new LMDBMapStandalone<Long,String>(LMDBSerializer.Long, LMDBSerializer.String);
    
    map.put(Long.MIN_VALUE, Long.valueOf(Long.MIN_VALUE).toString());
    map.put(Long.MAX_VALUE, Long.valueOf(Long.MAX_VALUE).toString());
    
    try (LMDBTxn txn = map.withReadWriteTxn()) {
      for (long i = -10_000L; i <= 10_000L; i++) {
        map.put(i, Long.valueOf(i).toString());
      }
    }
    
    return map;
  }
  
  private Long[] longStringMapKeys() {
    return concat(Long.valueOf(Long.MIN_VALUE), range(-10_000L, true, 10_000L, true), Long.valueOf(Long.MAX_VALUE));
  }
  
  @Test
  public void navigableMap() {
    try (LMDBMapStandalone<Long,String> map = makeLongStringMap()) {
      checkContents(map, longStringMapKeys());
      
      assertEquals(20003, map.keyCount());
      assertEquals(20003, map.valueCount());
      assertEquals(20003, map.size());
      
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
    // Note: We must retain a references to the LMDBMapStandalone so it doesn't get garbage collected and close
    //       all of our outstanding transactions before we are done using it.
    // TODO: Come up with a better way to handle this
    try (LMDBMapStandalone<Long,String> env = makeLongStringMap()) {
      try (LMDBMap<Long, String> map = env.descendingMap()) {
        assertEquals(20003, map.keyCount());
        assertEquals(20003, map.valueCount());
        assertEquals(20003, map.size());

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

        checkContents(map.subMap(10L, 0L), 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
        checkContents(map.subMap(10L, true, 0L, false), 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
        checkContents(map.subMap(10L, true, 0L, true), 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L);
        checkContents(map.subMap(10L, false, 0L, true), 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L);
        checkContents(map.subMap(10L, false, 0L, false), 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);

        checkContents(map.headMap(9_900L), concat(Long.valueOf(Long.MAX_VALUE), range(10_000L, true, 9_900L, true)));
        checkContents(map.headMap(9_900L, true), concat(Long.valueOf(Long.MAX_VALUE), range(10_000L, true, 9_900L, true)));
        checkContents(map.headMap(9_900L, false), concat(Long.valueOf(Long.MAX_VALUE), range(10_000L, true, 9_900L, false)));

        checkContents(map.tailMap(-9_900L), concat(range(-9_900L, false, -10_000L, true), Long.valueOf(Long.MIN_VALUE)));
        checkContents(map.tailMap(-9_900L, false), concat(range(-9_900L, false, -10_000L, true), Long.valueOf(Long.MIN_VALUE)));
        checkContents(map.tailMap(-9_900L, true), concat(range(-9_900L, true, -10_000L, true), Long.valueOf(Long.MIN_VALUE)));
      }
    }
  }
  
  @Test
  public void headMap() {
    // Note: We must retain a references to the LMDBMapStandalone so it doesn't get garbage collected and close
    //       all of our outstanding transactions before we are done using it.
    // TODO: Come up with a better way to handle this
    try (LMDBMapStandalone<Long,String> env = makeLongStringMap()) {
      try (LMDBMap<Long, String> map = env.headMap(1000L)) {
        assertEquals(11001, map.keyCount());
        assertEquals(11001, map.valueCount());
        assertEquals(11001, map.size());

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

        assertEquals("0", map.remove(0L));

        ceiling(map, 1L, 0L);
        higher(map, 1L, 0L);
        floor(map, -1L, 0L);
        lower(map, -1L, 0L);

        assertNull(map.put(-1_000_000L, "foo"));
        try { map.put(1_000_000L, "out of range"); fail(); } catch (LMDBOutOfRangeException ex) { /* good */ }
      }
    }
  }
  
  @Test
  public void tailMap() {
    // Note: We must retain a references to the LMDBMapStandalone so it doesn't get garbage collected and close
    //       all of our outstanding transactions before we are done using it.
    // TODO: Come up with a better way to handle this
    try (LMDBMapStandalone<Long,String> env = makeLongStringMap()) {
      try (LMDBMap<Long, String> map = env.tailMap(-1000L)) {
        assertEquals(11002, map.keyCount());
        assertEquals(11002, map.valueCount());
        assertEquals(11002, map.size());

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

        assertEquals("0", map.remove(0L));

        ceiling(map, 1L, 0L);
        higher(map, 1L, 0L);
        floor(map, -1L, 0L);
        lower(map, -1L, 0L);

        try { map.put(-1_000_000L, "out of range"); fail(); } catch (LMDBOutOfRangeException ex) { /* good */ }
        assertNull(map.put(1_000_000L, "in range"));
      }
    }
  }
  
  @Test
  public void subMap() {
    // Note: We must retain a references to the LMDBMapStandalone so it doesn't get garbage collected and close
    //       all of our outstanding transactions before we are done using it.
    // TODO: Come up with a better way to handle this
    try (LMDBMapStandalone<Long,String> env = makeLongStringMap()) {
      try (LMDBMap<Long, String> map = env.subMap(-1000L, 1000L)) {
        assertEquals(2000, map.keyCount());
        assertEquals(2000, map.valueCount());
        assertEquals(2000, map.size());

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

        assertEquals("0", map.remove(0L));

        ceiling(map, 1L, 0L);
        higher(map, 1L, 0L);
        floor(map, -1L, 0L);
        lower(map, -1L, 0L);

        try { map.put(-1_000_000L, "out of range"); fail(); } catch (LMDBOutOfRangeException ex) { /* good */ }
        try { map.put(1_000_000L, "out of range"); fail(); } catch (LMDBOutOfRangeException ex) { /* good */ }
      }
    }
  }

  
  private void first(LMDBMap<Long,String> map, Long expected) {
    assertEquals(expected, map.firstKey());
    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.firstEntry());
  }
  
  private void last(LMDBMap<Long,String> map, Long expected) {
    assertEquals(expected, map.lastKey());
    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.lastEntry());
  }
  
  private void ceiling(LMDBMap<Long,String> map, Long expected, Long arg) {
    assertEquals(expected, map.ceilingKey(arg));
    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.ceilingEntry(arg));
  }
  
  private void floor(LMDBMap<Long,String> map, Long expected, Long arg) {
    assertEquals(expected, map.floorKey(arg));
    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.floorEntry(arg));
  }
  
  private void higher(LMDBMap<Long,String> map, Long expected, Long arg) {
    assertEquals(expected, map.higherKey(arg));
    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.higherEntry(arg));
  }
  
  private void lower(LMDBMap<Long,String> map, Long expected, Long arg) {
    assertEquals(expected, map.lowerKey(arg));
    assertEquals(null == expected ? null : new SimpleImmutableEntry<Long,String>(expected, expected.toString()), map.lowerEntry(arg));
  }
  
//  private void checkContents(LMDBMap<Long,String> map, List<Long> keys) {
//    checkContents(map, keys.toArray(new Long[0]));
//  }
  
  private void checkContents(LMDBMap<Long,String> map, Long... keys) {
    checkContentsImpl(map, keys);
    checkContentsImpl(map.descendingMap(), reverse(keys));
    assertSame(map, map.descendingMap().descendingMap());
  }
  
  @SuppressWarnings("unchecked")
  private void checkContentsImpl(LMDBMap<Long,String> map, Long... keys) {
    final String[] vals = new String[keys.length];
    final SimpleImmutableEntry<Long,String>[] entries = new SimpleImmutableEntry[keys.length];
    
    for (int i = 0; i < keys.length; i++) {
      vals[i] = keys[i].toString();
      entries[i] = new SimpleImmutableEntry<Long,String>(keys[i], keys[i].toString());
    }

    assertEquals(keys.length, map.keyCount());
    assertEquals(keys.length, map.valueCount());
    assertEquals(keys.length, map.size());
    
    checkKeySet(keys, map.keySet());
    checkCollection(entries, map.entrySet());
    checkCollection(vals, map.values());
    
    checkKeySet(reverse(keys), map.keySet().descendingSet());
    
    // TODO: add subMap/headMap/tailMap testing here
    
    for (int i = 0; i < keys.length; i++) {
      ceiling(map, keys[i], keys[i]);
      floor(map, keys[i], keys[i]);
      lower(map, 0 == i ? null : keys[i-1], keys[i]);
      higher(map, keys.length - 1 == i ? null : keys[i+1], keys[i]);
    }
  }

}
