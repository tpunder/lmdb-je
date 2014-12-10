package eluvio.lmdb.map;

import java.io.File;
import java.util.Comparator;

/**
 * An {@link LMDBMap} implementation that is a self-contained LMDB environment
 * with a single database in it.
 * @param <K> map key
 * @param <V> map value
 */
public class LMDBMapStandalone<K,V> extends LMDBMapImpl<K,V> implements LMDBEnv {
  
  public LMDBMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer) {
    this(path, keySerializer, valueSerializer, null, false, DEFAULT_MAPSIZE);
  }
  
  public LMDBMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, boolean readOnly) {
    this(path, keySerializer, valueSerializer, null, readOnly, DEFAULT_MAPSIZE);
  }
  
  public LMDBMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, boolean readOnly, long mapsize) {
    this(path, keySerializer, valueSerializer, null, readOnly, mapsize);
  }
  
  public LMDBMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator) {
    this(path, keySerializer, valueSerializer, keyComparator, false, DEFAULT_MAPSIZE);
  }
  
  public LMDBMapStandalone(File path, LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, boolean readOnly, long mapsize) {
    super(new LMDBEnvImpl(path, readOnly, mapsize), keySerializer, valueSerializer, keyComparator);
  }
  
  public LMDBMapStandalone(LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer) {
    this(null, keySerializer, valueSerializer);
  }
  
  public LMDBMapStandalone(LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator) {
    this(null, keySerializer, valueSerializer, keyComparator);
  }
  
  public LMDBMapStandalone(LMDBSerializer<K> keySerializer, LMDBSerializer<V> valueSerializer, Comparator<K> keyComparator, long mapsize) {
    this(null, keySerializer, valueSerializer, keyComparator, false, mapsize);
  }

  @Override
  public void close() {
    env.closeTransactions();
    super.close();
    env.close();
  }
  
  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }
}
