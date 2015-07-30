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

import eluvio.lmdb.api.Api;

/**
 * Flags that can be passed to the underlying LMDB environment.
 * <p>
 * See the LMDB Documentation for more information about these flags:
 * <a href="http://symas.com/mdb/doc/group__mdb.html">http://symas.com/mdb/doc/group__mdb.html</a>
 */
public enum LMDBEnvFlag {
  /**
   * By default, LMDB creates its environment in a directory whose pathname is
   * given in path, and creates its data and lock files under that directory. 
   * With this option, path is used as-is for the database main data file.  The
   * database lock file is the path with "-lock" appended.
   * <p>
   * This sets the <b>MDB_NOSUBDIR</b> flag on the underlying LMDB environment.
   */
  NO_SUBDIR(Api.MDB_NOSUBDIR),
  
  /**
   * Open the environment in read-only mode. No write operations will be
   * allowed. LMDB will still modify the lock file - except on read-only
   * filesystems, where LMDB does not use locks.
   * <p>
   * This sets the <b>MDB_RDONLY</b> flag on the underlying LMDB environment.
   */
  READ_ONLY(Api.MDB_RDONLY),
  
  /**
   * Flush system buffers to disk only once per transaction, omit the metadata
   * flush. Defer that until the system flushes files to disk, or next
   * non-MDB_RDONLY commit or mdb_env_sync(). This optimization maintains
   * database integrity, but a system crash may undo the last committed
   * transaction. I.e. it preserves the ACI (atomicity, consistency, isolation)
   * but not D (durability) database property.
   * <p>
   * This sets the <b>MDB_NOMETASYNC</b> flag on the underlying LMDB environment.
   */
  NO_METADATA_SYNC(Api.MDB_NOMETASYNC),
  
  /**
   * Don't flush system buffers to disk when committing a transaction. This
   * optimization means a system crash can corrupt the database or lose the
   * last transactions if buffers are not yet flushed to disk. The risk is 
   * governed by how often the system flushes dirty buffers to disk and how
   * often mdb_env_sync() is called. However, if the filesystem preserves
   * write order, transactions exhibit ACI (atomicity, consistency, isolation)
   * properties and only lose D (durability). I.e. database integrity is 
   * maintained, but a system crash may undo the final transactions.
   * <p>
   * This sets the <b>MDB_NOSYNC</b> flag on the underlying LMDB environment.
   */
  NO_SYNC(Api.MDB_NOSYNC),
  
  /**
   * Turn off readahead. Most operating systems perform readahead on read
   * requests by default. This option turns it off if the OS supports it.
   * Turning it off may help random read performance when the DB is larger
   * than RAM and system RAM is full. The option is not implemented on Windows.
   * <p>
   * This sets the <b>MDB_NORDAHEAD</b> flag on the underlying LMDB environment.
   */
  
  NO_READAHEAD(Api.MDB_NORDAHEAD),
  /**
   * Don't initialize malloc'd memory before writing to unused spaces in the
   * data file. By default, memory for pages written to the data file is
   * obtained using malloc. While these pages may be reused in subsequent
   * transactions, freshly malloc'd pages will be initialized to zeroes before
   * use. This avoids persisting leftover data from other code (that used the
   * heap and subsequently freed the memory) into the data file. Note that many
   * other system libraries may allocate and free memory from the heap for
   * arbitrary uses. E.g., stdio may use the heap for file I/O buffers. This
   * initialization step has a modest performance cost so some applications may
   * want to disable it using this flag. This option can be a problem for
   * applications which handle sensitive data like passwords, and it makes
   * memory checkers like Valgrind noisy. This flag is not needed with
   * MDB_WRITEMAP, which writes directly to the mmap instead of using malloc
   * for pages. The initialization is also skipped if MDB_RESERVE is used; the
   * caller is expected to overwrite all of the memory that was reserved in
   * that case.
   * <p>
   * This sets the <b>MDB_NOMEMINIT</b> flag on the underlying LMDB environment.
   */
  NO_MEM_INIT(Api.MDB_NORDAHEAD);
  
  /**
   * The corresponding LMDB flag.
   */
  protected final int lmdbValue;
  
  private LMDBEnvFlag(int lmdbValue) {
    this.lmdbValue = lmdbValue;
  }
}
