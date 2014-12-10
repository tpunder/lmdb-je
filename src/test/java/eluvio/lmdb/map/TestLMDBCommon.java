package eluvio.lmdb.map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

public class TestLMDBCommon {
  
  @Test
  public void testAssertionsEnabled() {
    try {
      assert false;
      fail();
    } catch(AssertionError ex) {
      // good
    }
  }
  
  protected <T> void checkKeySet(T[] expected, LMDBKeySet<T> set) {
    checkCollection(expected, set);
    
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], set.ceiling(expected[i]));
      assertEquals(expected[i], set.floor(expected[i]));
      assertEquals(0 == i ? null : expected[i-1], set.lower(expected[i]));
      assertEquals(expected.length - 1 == i ? null : expected[i+1], set.higher(expected[i]));
    }
  }
  
  protected <T> void checkCollection(T[] expected, LMDBCollection<T> col) {    
    assertEquals(expected.length, col.size());
    assertArrayEquals(expected, col.toArray());
    assertArrayEquals(expected, col.toArray(new Object[0]));
    assertArrayEquals(expected, col.toArray(new Object[expected.length]));
    assertArrayEquals(expected, col.toArray(new Object[expected.length-1]));
    assertArrayEquals(Arrays.copyOf(expected, expected.length+1), col.toArray(new Object[expected.length+1]));
    checkIterable(expected, col);
  }
  
  protected <T> void checkIterable(T[] expected, LMDBIterable<T> col) {
    try (LMDBIterator<T> it = col.lmdbIterator()) {
      for (int i = 0; i < expected.length; i ++) {
        assertTrue("Iterator has less elements than expected!", it.hasNext());
        assertEquals(expected[i], it.next());
      }
      assertFalse("Iterator has more elements?!", it.hasNext());
    }
  }
  
  @SuppressWarnings("unchecked")
  protected <T> T[] reverse(T[] arr) {
    T[] rev = (T[])java.lang.reflect.Array.newInstance(arr.getClass().getComponentType(), arr.length);
    
    for(int i = 0; i < arr.length; i++) {
      rev[i] = arr[arr.length - i - 1];
    }
    
    return rev;
  }
  
  @SuppressWarnings("unchecked")
  protected <T> T[] concat(T a, T[] arr, T b) {
    ArrayList<T> list = new ArrayList<T>(arr.length + 2);
    list.add(a);
    list.addAll(Arrays.asList(arr));
    list.add(b);
    return list.toArray((T[])java.lang.reflect.Array.newInstance(arr.getClass().getComponentType(), list.size()));
  }
  
  @SuppressWarnings("unchecked")
  protected <T> T[] concat(T v, T[] arr) {
    ArrayList<T> list = new ArrayList<T>(arr.length + 1);
    list.add(v);
    list.addAll(Arrays.asList(arr));
    return list.toArray((T[])java.lang.reflect.Array.newInstance(arr.getClass().getComponentType(), list.size()));
  }
  
  @SuppressWarnings("unchecked")
  protected <T> T[] concat(T[] arr, T v) {
    ArrayList<T> list = new ArrayList<T>(arr.length + 1);
    list.addAll(Arrays.asList(arr));
    list.add(v);
    return list.toArray((T[])java.lang.reflect.Array.newInstance(arr.getClass().getComponentType(), list.size()));
  }
  
  // Make sure our range() method is correct
  @Test
  public void testRange() {
    assertArrayEquals(new Long[]{ 1L, 2L, 3L, 4L }, range(1L, 5L));
    assertArrayEquals(new Long[]{ 1L, 2L, 3L, 4L, 5L }, range(1L, true, 5L, true));
    assertArrayEquals(new Long[]{ 2L, 3L, 4L, 5L }, range(1L, false, 5L, true));
    assertArrayEquals(new Long[]{ 2L, 3L, 4L }, range(1L, false, 5L, false));
    
    assertArrayEquals(new Long[]{ 5L, 4L, 3L, 2L }, range(5L, 1L));
    assertArrayEquals(new Long[]{ 5L, 4L, 3L, 2L, 1L }, range(5L, true, 1L, true));
    assertArrayEquals(new Long[]{ 4L, 3L, 2L, 1L }, range(5L, false, 1L, true));
    assertArrayEquals(new Long[]{ 4L, 3L, 2L }, range(5L, false, 1L, false));
    
    assertArrayEquals(new Long[]{ -5L, -4L, -3L, -2L, -1L, 0L, 1L, 2L, 3L, 4L }, range(-5L, 5L));
    assertArrayEquals(new Long[]{ 5L, 4L, 3L, 2L, 1L, 0L, -1L, -2L, -3L, -4L }, range(5L, -5L));
  }
  
  protected Long[] range(long from, long to) {
    return range(from, true, to, false);
  }
  
  protected Long[] range(long from, boolean fromInclusive, long to, boolean toInclusive) {
    if (from < to) {
      if (!fromInclusive) from++;
      if (!toInclusive) to--; 
    } else {
      if (!fromInclusive) from--;
      if (!toInclusive) to++;
    }
    
    int size = (int)Math.abs(from - to) + 1;
    Long[] arr = new Long[size];
    
    for (int i = 0; i < size; i++) arr[i] = from < to ? from + i : from - i;

    return arr;
  }
}
