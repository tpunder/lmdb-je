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