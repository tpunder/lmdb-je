package eluvio.lmdb.api;

public class EnvInfo {
  public final long mapAddress;
  public final long mapSize;
  public final long lastPageNumber;
  public final long lastTxnId;
  public final long maxReaders;
  public final long numReaders;

  protected EnvInfo(Api.MDB_envinfo info) {
    mapAddress = info.me_mapaddr.longValue();
    mapSize = info.me_mapsize.longValue();
    lastPageNumber = info.me_last_pgno.longValue();
    lastTxnId = info.me_last_txnid.longValue();
    maxReaders = info.me_maxreaders.longValue();
    numReaders = info.me_numreaders.longValue();
  }
  
  public String toString() {
    return "EnvInfo(mapAddress: "+mapAddress+", mapSize: "+mapSize+", lastPageNumber: "+lastPageNumber+", lastTxnId: "+lastTxnId+", maxReaders: "+maxReaders+", numReaders: "+numReaders+")";
  }
}