package eluvio.lmdb.map;

public interface LMDBTxn extends AutoCloseable {
  void abort();
  void close();
  void commit();
}
