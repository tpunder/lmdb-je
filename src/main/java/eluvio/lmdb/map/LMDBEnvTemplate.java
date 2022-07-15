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

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public abstract class LMDBEnvTemplate implements LMDBEnv {
  private final static int DEFAULT_MAX_DBS = 32;
  
  private final LMDBEnvImpl env;
  private final Set<LMDBMap<?,?>> maps;
  private final Set<LMDBMultiMap<?,?>> multiMaps;
  
  public LMDBEnvTemplate() {
    this(null, false, LMDBEnv.DEFAULT_MAPSIZE);
  }
  
  public LMDBEnvTemplate(File path) {
    this(path, false /* readOnly */, LMDBEnv.DEFAULT_MAPSIZE);
  }
  
  public LMDBEnvTemplate(File path, boolean readOnly) {
    this(path, readOnly, LMDBEnv.DEFAULT_MAPSIZE);
  }
  
  public LMDBEnvTemplate(File path, boolean readOnly, long mapsize) {
    this(path, readOnly, mapsize, DEFAULT_MAX_DBS);
  }
  
  public LMDBEnvTemplate(File path, boolean readOnly, long mapsize, int maxdbs) {
    this(path, readOnly, mapsize, maxdbs, LMDBEnv.DEFAULT_MAXREADERS);
  }

  public LMDBEnvTemplate(File path, boolean readOnly, long mapsize, int maxdbs, int maxReaders) {
    this(path, readOnly, mapsize, maxdbs, maxReaders, 0);
  }

  public LMDBEnvTemplate(File path, boolean readOnly, long mapsize, int maxdbs, int maxReaders, int flags) {
    this.env = new LMDBEnvImpl(path, readOnly, mapsize, maxdbs, maxReaders, flags);
    this.maps = Collections.synchronizedSet(new HashSet<LMDBMap<?,?>>());
    this.multiMaps = Collections.synchronizedSet(new HashSet<LMDBMultiMap<?,?>>());
  }
  
  protected class LMDBMapTemplate<K,V> extends LMDBMapImpl<K,V> {
    public LMDBMapTemplate(String name, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer) {
      this(name, keySerializer, valueSerializer, null);
    }
    
    public LMDBMapTemplate(String name, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator) {
      super(LMDBEnvTemplate.this.env, name, keySerializer, valueSerializer, keyComparator, null /* valueComparator */, false /* dup */);
      LMDBEnvTemplate.this.maps.add(this);
    }
  }
  
  protected class LMDBMultiMapTemplate<K,V> extends LMDBMultiMapImpl<K,V> {
    public LMDBMultiMapTemplate(String name, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer) {
      this(name, keySerializer, valueSerializer, null);
    }
    
    public LMDBMultiMapTemplate(String name, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator) {
      this(name, keySerializer, valueSerializer, keyComparator, null);
    }
    
    public LMDBMultiMapTemplate(String name, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, Comparator<V> valueComparator) {
      super(LMDBEnvTemplate.this.env, name, keySerializer, valueSerializer, keyComparator, valueComparator);
      LMDBEnvTemplate.this.multiMaps.add(this);
    }
  }

  @Override
  final public void close() {
    env.closeTransactions();
    
    for(LMDBMap<?,?> map : maps){
      map.close();
    }
    
    for(LMDBMultiMap<?,?> multiMap : multiMaps){
      multiMap.close();
    }
    
    env.close();
  }
  
  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }
  
  @Override
  final public void abortTxn() {
    env.abortTxn();
  }

  @Override
  final public void beginTxn() {
    env.beginTxn();
  }

  @Override
  final public void beginTxn(boolean readOnly) {
    env.beginTxn(readOnly);
  }

  @Override
  final public void commitTxn() {
    env.commitTxn();
  }

  @Override
  final public ReusableTxn detachTxnFromCurrentThread() {
    return env.detachTxnFromCurrentThread();
  }

  @Override
  public boolean readOnly() {
    return env.readOnly();
  }

  @Override
  final public LMDBTxn withExistingReadOnlyTxn() {
    return env.withExistingReadOnlyTxn();
  }

  @Override
  final public LMDBTxn withExistingReadWriteTxn() {
    return env.withExistingReadWriteTxn();
  }

  @Override
  final public LMDBTxn withExistingTxn() {
    return env.withExistingTxn();
  }

  @Override
  final public LMDBTxn withNestedReadWriteTxn() {
    return env.withNestedReadWriteTxn();
  }

  @Override
  final public LMDBTxn withReadOnlyTxn() {
    return env.withReadOnlyTxn();
  }

  @Override
  final public LMDBTxn withReadWriteTxn() {
    return env.withReadWriteTxn();
  }
  
  @Override
  public void disableMetaSync() {
    env.disableMetaSync();
  }
  
  @Override
  public void enableMetaSync() {
    env.enableMetaSync();
  }
  
  @Override
  public void disableSync() {
    env.disableSync();
  }
  
  @Override
  public void enableSync() {
    env.enableSync();
  }

  @Override
  public void sync() { env.sync(); }

  @Override
  public void sync(boolean force) { env.sync(force); }
}
