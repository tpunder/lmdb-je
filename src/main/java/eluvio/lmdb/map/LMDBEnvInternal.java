package eluvio.lmdb.map;

import eluvio.lmdb.api.Env;

abstract class LMDBEnvInternal implements LMDBEnv {
  abstract void closeTransactions();
  
  abstract Env env();
  
  @Override
  public abstract LMDBTxnInternal withExistingReadOnlyTxn();
  
  @Override
  public abstract LMDBTxnInternal withExistingReadWriteTxn();
  
  @Override
  public abstract LMDBTxnInternal withExistingTxn();
  
  @Override
  public abstract LMDBTxnInternal withNestedReadWriteTxn();
  
  @Override
  public abstract LMDBTxnInternal withReadOnlyTxn();
  
  @Override
  public abstract LMDBTxnInternal withReadWriteTxn();
}
