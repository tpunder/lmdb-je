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
  
  public LMDBEnvTemplate(File path, boolean readOnly) {
    this(path, readOnly, LMDBEnv.DEFAULT_MAPSIZE);
  }
  
  public LMDBEnvTemplate(File path, boolean readOnly, long mapsize) {
    this(path, readOnly, mapsize, DEFAULT_MAX_DBS);
  }
  
  public LMDBEnvTemplate(File path, boolean readOnly, long mapsize, int maxdbs) {
    this.env = new LMDBEnvImpl(path, readOnly, mapsize, maxdbs);
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
}
