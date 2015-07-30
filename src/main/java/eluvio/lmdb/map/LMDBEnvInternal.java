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
