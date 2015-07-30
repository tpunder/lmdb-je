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
package eluvio.lmdb.api;

import java.io.File;
import java.util.Comparator;
import java.util.Map;

import eluvio.lmdb.map.LMDBMap;
import eluvio.lmdb.map.LMDBComparator;
import eluvio.lmdb.map.LMDBIterator;
import eluvio.lmdb.map.LMDBMapStandalone;
import eluvio.lmdb.map.LMDBSerializer;

public class Test {
  public static void main(String[] args) {
    benchWrite("Warm", 100000);
    benchWrite("Bench", 1000000);
  }
  
//  public static void mainThreadTest(String[] args) {
//    final LMDBStringMap map = new LMDBStringMap("/Users/tim/tmp/lmdb_byte_db");
//    
//    WriterThread thread1 = new WriterThread(1, map);
//    WriterThread thread2 = new WriterThread(2, map);
//    WriterThread thread3 = new WriterThread(3, map);
//    WriterThread thread4 = new WriterThread(4, map);
//    WriterThread thread5 = new WriterThread(5, map);
//    
//    thread1.start();
//    thread2.start();
//    thread3.start();
//    thread4.start();
//    thread5.start();
//    
//    try { thread1.join(); } catch (InterruptedException e) { e.printStackTrace(); }
//    try { thread2.join(); } catch (InterruptedException e) { e.printStackTrace(); }
//    try { thread3.join(); } catch (InterruptedException e) { e.printStackTrace(); }
//    try { thread4.join(); } catch (InterruptedException e) { e.printStackTrace(); }
//    try { thread5.join(); } catch (InterruptedException e) { e.printStackTrace(); }
//    
//    System.out.println("");
//    System.out.println("Entries:");
//    
//    try (LMDBStringMap.EntryIterator it = map.entrySet().iterator()) {
//      while (it.hasNext()) {
//        Map.Entry<String,String> entry = it.next();
//        System.out.println("  \""+entry.getKey()+"\" => \""+entry.getValue()+"\"");
//      }
//    }
//  }
  
  public static void main0(String[] args) {
    //final Comparator<String> comparator = String.CASE_INSENSITIVE_ORDER;
    //final Comparator<String> comparator = Comparator.<String>naturalOrder();
    //final LMDBMap<String,String> map = new LMDBMap<String,String>(new File("/Users/tim/tmp/lmdb_byte_db"), LMDBMap.StringSerializer, LMDBMap.StringSerializer, comparator);
    //final LMDBMapImpl<String,String> map = new LMDBMapImpl<String,String>(null, LMDBMapImpl.StringSerializer, LMDBMapImpl.StringSerializer, comparator);
    final LMDBMapStandalone<String,String> map = new LMDBMapStandalone<String,String>(null, LMDBSerializer.String, LMDBSerializer.String);
        
    map.put("A", "A");
    map.put("b", "b");
    map.put("C", "C");
    map.put("d", "d");
    
    try (LMDBIterator<Map.Entry<String,String>> it = map.entrySet().lmdbIterator()) {
      while (it.hasNext()) {
        Map.Entry<String,String> entry = it.next();
        System.out.println("  \""+entry.getKey()+"\" => \""+entry.getValue()+"\"");
      }
    }
    
    map.close();
  }
  
  private static void benchWrite(String msg, int count) {
    final long start = System.currentTimeMillis();

//    try (final LMDBMapStandAlone<String,String> map = new LMDBMapStandAlone<String,String>(LMDBMapImpl.StringSerializer, LMDBMap.StringSerializer)) {
    try (final LMDBMapStandalone<String,String> map = new LMDBMapStandalone<String,String>(new File("/Users/tim/tmp/lmdb_byte_db"), LMDBSerializer.String, LMDBSerializer.String)) {
//    try (final LMDBMapStandAlone<String,String> map = new LMDBMapStandAlone<String,String>(new File("/Users/tim/tmp/lmdb_byte_db"), LMDBMap.StringSerializer, LMDBMap.StringSerializer, String.CASE_INSENSITIVE_ORDER)) {
//    try (final LMDBMapStandAlone<String,String> map = new LMDBMapStandAlone<String,String>(new File("/Users/tim/tmp/lmdb_byte_db"), LMDBMap.StringSerializer, LMDBMap.StringSerializer, LMDBMapComparator.CASE_SENSITIVE_STRING_COMPARATOR)) {
      final String keyPrefix = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";
      final String valuePrefix = "Donec ac odio lacus. Nullam finibus vehicula magna id bibendum. Mauris mattis neque eleifend nisl finibus, et commodo sem suscipit. Praesent ac suscipit lorem, a congue felis. Proin at tellus fermentum, posuere massa id, condimentum sapien. Vestibulum quis leo quam. Aenean ut enim eleifend, feugiat magna sed, aliquam ligula. Proin placerat eros vitae augue laoreet, non finibus neque ullamcorper. Donec purus elit, faucibus at augue ac, rhoncus finibus enim. Morbi ac suscipit dui, vel eleifend arcu. Etiam ipsum velit, rutrum et nisl a, eleifend tristique eros. Mauris imperdiet lacus non pretium ullamcorper. Praesent dictum tempor mauris, efficitur elementum sapien elementum at. Maecenas rhoncus mollis lorem, sed semper nisl ornare porta.";
      
      map.beginTxn();
      for (int i = 0; i < count; i++) {
        //map.put(keyPrefix+i, valuePrefix+i, false);
        map.putNoPrev(keyPrefix+i, valuePrefix);
        
        if (i % 10000 == 0) {
          map.commitTxn();
          map.beginTxn();
        }
      }
      map.commitTxn();
    }
    
    final long end = System.currentTimeMillis();
    final long ms = end-start;
    final double perSecond = Math.round((double)count / (double)ms * (double)1000);
    System.out.println(msg+" ("+count+") - "+ms+"ms  ("+perSecond+"/s)");
  }
  
//  private static class WriterThread extends Thread {
//    private final int id;
//    private final LMDBStringMap map;
//    
//    public WriterThread(int id, LMDBStringMap map) {
//      this.id = id;
//      this.map = map;
//    }
//    
//    public void run() {
//      try {
//        System.out.println(id+" - Attempting to beginTxn()");
//        map.beginTxn();
//        System.out.println(id+" - Txn started.");
//        Thread.sleep(250);
//        final long now = System.currentTimeMillis();
//        System.out.println(id+" - map.get(\"foo\"): "+map.get("foo"));
//        Thread.sleep(250);
//        System.out.println(id+" - map.put(\"foo\",\"bar - "+now+"\"): "+map.put("foo", "bar - "+now));
//        Thread.sleep(250);
//        System.out.println(id+" - map.get(\"foo\"): "+map.get("foo"));
//        Thread.sleep(250);
//        System.out.println(id+" - map.put(\"foo\",\"Hello World - "+now+"\"): "+map.put("foo", "Hello World - "+now));
//        Thread.sleep(250);
//        System.out.println(id+" - map.get(\"foo\"): "+map.get("foo"));
//        Thread.sleep(250);
//        System.out.println(id+" - map.put(\"bar\",\"asdasdasd - \""+now+"): "+map.put("bar", "asdasdasd - "+now));
//        Thread.sleep(250);
//        System.out.println(id+" - About to commitTxn()");
//        map.commitTxn();
//        System.out.println(id+" - Txn completed.");
//      } catch (InterruptedException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      }
//    }
//  }
 
}
