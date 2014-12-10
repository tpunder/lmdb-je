package eluvio.lmdb.map;

import eluvio.lmdb.api.Txn;

abstract class LMDBTxnInternal implements LMDBTxn {
  abstract Txn txn();
  abstract boolean readOnly();
}
