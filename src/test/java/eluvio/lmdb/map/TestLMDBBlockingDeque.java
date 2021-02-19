/*
 * Copyright 2021 Tim Underwood (https://github.com/tpunder)
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

import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestLMDBBlockingDeque {

  @Test
  public void unsignedLongComparator() {
    try (LMDBMapStandalone<Long,String> map = new LMDBMapStandalone<Long, String>(LMDBSerializer.UnsignedLong, LMDBSerializer.String)) {
      // This is not currently supported. You must use LMDBSerializer.Long.
      assertThrows(IllegalArgumentException.class, () -> new LMDBBlockingDeque<String>(map));
    }
  }

  @Test
  public void unboundedBasics() {
    try (LMDBBlockingDequeStandalone<String> queue = new LMDBBlockingDequeStandalone<String>(LMDBSerializer.String)) {
      assertEquals(queue.capacity, -1);
      assertEquals(queue.remainingCapacity(), Integer.MAX_VALUE);
      assertFalse(queue.readOnly());

      assertThrows(NoSuchElementException.class, () -> queue.removeFirst());
      assertThrows(NoSuchElementException.class, () -> queue.removeLast());
      assertThrows(NoSuchElementException.class, () -> queue.getFirst());
      assertThrows(NoSuchElementException.class, () -> queue.getLast());
      
      assertNull(queue.pollFirst());
      assertNull(queue.pollLast());

      assertEquals(queue.size(), 0);
      assertNull(queue.peekFirst());
      assertNull(queue.peekLast());
      assertFalse(queue.contains("first"));
      assertFalse(queue.contains("last"));

      queue.addFirst("first");

      assertEquals(queue.remainingCapacity(), Integer.MAX_VALUE);
      assertEquals(queue.size(), 1);
      assertEquals(queue.peekFirst(), "first");
      assertEquals(queue.peekLast(), "first");
      assertTrue(queue.contains("first"));
      assertFalse(queue.contains("last"));

      queue.addLast("last");

      assertEquals(queue.remainingCapacity(), Integer.MAX_VALUE);
      assertEquals(queue.size(), 2);
      assertEquals(queue.peekFirst(), "first");
      assertEquals(queue.peekLast(), "last");
      assertTrue(queue.contains("first"));
      assertTrue(queue.contains("last"));

      assertEquals(queue.removeFirst(), "first");

      assertEquals(queue.size(), 1);
      assertEquals(queue.peekFirst(), "last");
      assertEquals(queue.peekLast(), "last");
      assertFalse(queue.contains("first"));
      assertTrue(queue.contains("last"));

      assertEquals(queue.removeFirst(), "last");

      assertEquals(queue.size(), 0);
      assertNull(queue.peekFirst());
      assertNull(queue.peekLast());
      assertFalse(queue.contains("first"));
      assertFalse(queue.contains("last"));

      assertNull(queue.pollFirst());
      assertNull(queue.pollLast());
    }
  }

  @Test
  public void boundedBasics() {
    try (LMDBBlockingDequeStandalone<String> queue = new LMDBBlockingDequeStandalone<String>(LMDBSerializer.String, 1)) {
      assertEquals(queue.capacity, 1);
      assertEquals(queue.remainingCapacity(), 1);

      queue.addFirst("first");

      assertEquals(queue.remainingCapacity(), 0);
      assertThrows(IllegalStateException.class, () -> queue.addFirst("bad"));
      assertThrows(IllegalStateException.class, () -> queue.addLast("bad"));
      assertFalse(queue.offerFirst("bad"));
      assertFalse(queue.offerLast("bad"));
      assertEquals(queue.peekFirst(), "first");
      assertEquals(queue.peekLast(), "first");

      assertEquals(queue.removeFirst(), "first");
      assertEquals(queue.remainingCapacity(), 1);

      queue.addFirst("second");

      assertEquals(queue.remainingCapacity(), 0);
      assertThrows(IllegalStateException.class, () -> queue.addFirst("bad"));
      assertThrows(IllegalStateException.class, () -> queue.addLast("bad"));
      assertFalse(queue.offerFirst("bad"));
      assertFalse(queue.offerLast("bad"));
      assertEquals(queue.peekFirst(), "second");
      assertEquals(queue.peekLast(), "second");
      assertEquals(queue.removeFirst(), "second");
    }
  }

  @Test
  public void threadingBasics() throws Throwable {
    try (LMDBBlockingDequeStandalone<String> queue = new LMDBBlockingDequeStandalone<String>(LMDBSerializer.String, 1)) {

      final WaitableThread takeFirstThread = startThread(() -> assertEquals(queue.takeFirst(), "first"));

      Thread.sleep(100);
      assertTrue(takeFirstThread.isAlive());

      queue.addFirst("first");
      takeFirstThread.await();

      assertEquals(queue.size(), 0);

      queue.addFirst("first");

      final WaitableThread putFirstThread = startThread(() -> queue.putFirst("second"));

      Thread.sleep(100);
      assertTrue(putFirstThread.isAlive());
      assertEquals(queue.peekFirst(), "first");
      assertEquals(queue.takeFirst(), "first");

      putFirstThread.await();

      assertEquals(queue.size(), 1);
      assertEquals(queue.peekFirst(), "second");
      assertEquals(queue.takeFirst(), "second");
    }
  }

  @FunctionalInterface
  private interface RunnableWithThrowable {
    void run() throws Throwable;
  }

  private WaitableThread startThread(RunnableWithThrowable target) {
    final WaitableThread thread = new WaitableThread(target);
    thread.start();
    return thread;
  }

  private class WaitableThread extends Thread {
    final RunnableWithThrowable target;
    final CountDownLatch latch = new CountDownLatch(1);
    volatile Throwable ex;

    public WaitableThread(RunnableWithThrowable target) {
      super();
      this.target = target;
    }

    @Override
    public void run() {
      try {
        target.run();
      } catch (Throwable ex) {
        this.ex = ex;
      } finally {
        latch.countDown();
      }
    }

    public void await() throws Throwable {
      latch.await();
      if (null != ex) throw ex;
    }

    public void await(long timeout, TimeUnit unit) throws Throwable {
      if (latch.await(timeout, unit) && null != ex) throw ex;
    }
  }
}
