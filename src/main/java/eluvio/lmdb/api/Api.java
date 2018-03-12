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

import com.kenai.jffi.MemoryIO;

import java.nio.ByteBuffer;

import jnr.ffi.*;
import jnr.ffi.annotations.*;
import jnr.ffi.byref.*;
import jnr.ffi.types.*;

/**
 * The interface for making calls to the underlying LMDB library.
 * <p>
 * See the LMDB Site for documention on these calls: <a target="_blank" href="http://symas.com/mdb/doc/group__mdb.html">http://symas.com/mdb/doc/group__mdb.html</a>
 */
public interface Api {
  /**
   * The JNR implemented instance of Api 
   */
  public final static Api instance = Loader.load();
  
  /**
   * The JNR Runtime that goes with the instance
   */
  public final static jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getRuntime(instance);
  
  // Environment Flags
  public final static int MDB_FIXEDMAP   = 0x01;
  public final static int MDB_NOSUBDIR   = 0x4000;
  public final static int MDB_NOSYNC     = 0x10000;
  public final static int MDB_RDONLY     = 0x20000;
  public final static int MDB_NOMETASYNC = 0x40000;
  public final static int MDB_WRITEMAP   = 0x80000;
  public final static int MDB_MAPASYNC   = 0x100000;
  public final static int MDB_NOTLS      = 0x200000;
  public final static int MDB_NOLOCK     = 0x400000;
  public final static int MDB_NORDAHEAD  = 0x800000;
  public final static int MDB_NOMEMINIT  = 0x1000000;
    
  // Database Flags
  public final static int MDB_REVERSEKEY = 0x02;
  public final static int MDB_DUPSORT    = 0x04;
  public final static int MDB_INTEGERKEY = 0x08;
  public final static int MDB_DUPFIXED   = 0x10;
  public final static int MDB_INTEGERDUP = 0x20;
  public final static int MDB_REVERSEDUP = 0x40;
  public final static int MDB_CREATE     = 0x40000;
 
  // Write Flags
  public final static int MDB_NOOVERWRITE = 0x10;
  public final static int MDB_NODUPDATA   = 0x20;
  public final static int MDB_CURRENT     = 0x40;
  public final static int MDB_RESERVE     = 0x10000;
  public final static int MDB_APPEND      = 0x20000;
  public final static int MDB_APPENDDUP   = 0x40000;
  public final static int MDB_MULTIPLE    = 0x80000;
  
  // Copy Flags
  public final static int MDB_CP_COMPACT = 0x01;
  
  public static class MDB_val extends Struct {
    public final size_t mv_size = new size_t();
    public final Pointer mv_data = new Pointer();

    MDB_val(jnr.ffi.Pointer address) {
      this();
      useMemory(address);
    }
    
    MDB_val(ByteBuffer buf) {
      this();
      if (null != buf) {
        if (!buf.isDirect()) throw new IllegalArgumentException("You must use Direct ByteBuffers (possibly a bug with jnr-ffi)");
        mv_size.set(buf.remaining());
        mv_data.set(jnr.ffi.Pointer.wrap(runtime, buf));
      }
    }
    
    MDB_val(long size) {
      this();
      mv_size.set(size);
    }
    
    MDB_val() {
      super(runtime);
    }
    
    public ByteBuffer asByteBuffer() {
      long size = mv_size.longValue();
      jnr.ffi.Pointer pointer = mv_data.get();
      
      if (size < 0 || size > Integer.MAX_VALUE) throw new IndexOutOfBoundsException("Can't create ByteBuffer with size: "+size);
      if (null == pointer) return null;
      if (!pointer.isDirect()) throw new IllegalArgumentException("Expected the Pointer to be a direct pointer");
      
      return MemoryIO.getInstance().newDirectByteBuffer(pointer.address(), (int)size);
    }
  }
  
  public static class MDB_stat extends Struct {
    public final Unsigned32 ms_psize = new Unsigned32();
    public final Unsigned32 ms_depth = new Unsigned32();
    public final size_t ms_branch_pages = new size_t();
    public final size_t ms_leaf_pages = new size_t();
    public final size_t ms_overflow_pages = new size_t();
    public final size_t ms_entries = new size_t();
    
    MDB_stat() {
      super(runtime);
    }
  }
  
  public static class MDB_envinfo extends Struct {
    public final Pointer me_mapaddr = new Pointer();
    public final size_t me_mapsize = new size_t();
    public final size_t me_last_pgno = new size_t();
    public final size_t me_last_txnid = new size_t();
    public final Unsigned32 me_maxreaders = new Unsigned32();
    public final Unsigned32 me_numreaders = new Unsigned32();
    
    MDB_envinfo() {
      super(runtime);
    }
  }
  
  public static interface MDB_cmp_func {
    // typedef int(MDB_cmp_func)(const MDB_val *a, const MDB_val *b)
    @Delegate public int call(Pointer a, Pointer b);
  }
  
  @IgnoreError String mdb_version(@Out IntByReference major, @Out IntByReference minor, @Out IntByReference patch);
  @IgnoreError String mdb_strerror(int err);
  
  @IgnoreError int mdb_env_create(@Out PointerByReference env);
  @IgnoreError int mdb_env_open(Pointer env, @In CharSequence path, int flags, @mode_t int mode);
  @IgnoreError void mdb_env_close(Pointer env);
  @IgnoreError int mdb_env_set_mapsize(Pointer env, @size_t long size);
  @IgnoreError int mdb_env_set_maxreaders(Pointer env, int readers);
  @IgnoreError int mdb_env_set_maxdbs(Pointer env, int dbs);
  @IgnoreError int mdb_env_get_maxkeysize(Pointer env);
  @IgnoreError int mdb_env_sync(Pointer env, int force);
  @IgnoreError int mdb_env_stat(Pointer env, @Out @Transient MDB_stat stat);
  @IgnoreError int mdb_env_info(Pointer env, @Out @Transient MDB_envinfo stat);
  @IgnoreError int mdb_env_set_flags(Pointer env, int flags, int onoff);
  @IgnoreError int mdb_env_get_flags(Pointer env, @Out IntByReference flags);
  
  @IgnoreError int mdb_txn_begin(Pointer env, Pointer parent, int flags, @Out PointerByReference txn);
  @IgnoreError int mdb_txn_commit(Pointer txn);
  @IgnoreError void mdb_txn_abort(Pointer txn);
  @IgnoreError void mdb_txn_reset(Pointer txn);
  @IgnoreError void mdb_txn_renew(Pointer txn);
  
  @IgnoreError int mdb_dbi_open(Pointer txn, @In CharSequence name, int flags, @Out IntByReference dbi);
  @IgnoreError void mdb_dbi_close(Pointer env, int dbi);
  @IgnoreError int mdb_dbi_flags(Pointer txn, int dbi, @Out IntByReference flags);

  @IgnoreError int mdb_set_compare(Pointer txn, int dbi, MDB_cmp_func cmp);
  @IgnoreError int mdb_set_dupsort(Pointer txn, int dbi, MDB_cmp_func cmp);
  
  @IgnoreError int mdb_cmp(Pointer txn, int dbi, @In MDB_val a, @In MDB_val b);
  @IgnoreError int mdb_dcmp(Pointer txn, int dbi, @In MDB_val a, @In MDB_val b);
  
  @IgnoreError int mdb_get(Pointer txn, int dbi, @In MDB_val key, @Out MDB_val data);
  @IgnoreError int mdb_put(Pointer txn, int dbi, @In MDB_val key, /* in|out with MDB_RESERVE */ MDB_val data, int flags);
  @IgnoreError int mdb_del(Pointer txn, int dbi, @In MDB_val key, @In MDB_val data);
  @IgnoreError int mdb_drop(Pointer txn, int dbi, int del);
  @IgnoreError int mdb_stat(Pointer env, int dbi, @Out @Transient MDB_stat stat);
  
  @IgnoreError int mdb_cursor_open(Pointer txn, int dbi, @Out PointerByReference cursor);
  @IgnoreError void mdb_cursor_close(Pointer cursor);
  @IgnoreError int mdb_cursor_renew(Pointer txn, Pointer cursor);
  @IgnoreError int mdb_cursor_get(Pointer cursor, MDB_val key, MDB_val data, CursorOp op);
  @IgnoreError int mdb_cursor_put(Pointer cursor, MDB_val key, MDB_val data, int flags);
  @IgnoreError int mdb_cursor_del(Pointer cursor, int flags);
  @IgnoreError int mdb_cursor_count(Pointer cursor, @Out NumberByReference countp);
  
  @IgnoreError int mdb_reader_check(Pointer env, @Out IntByReference dead);
}