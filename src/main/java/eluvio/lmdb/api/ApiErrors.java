package eluvio.lmdb.api;

import eluvio.lmdb.api.LMDBException;
import static eluvio.lmdb.api.LMDBException.*;

public final class ApiErrors {
  final public static int MDB_SUCCESS = 0;
  final public static int MDB_KEYEXIST = -30799;
  final public static int MDB_NOTFOUND = -30798;
  final public static int MDB_PAGE_NOTFOUND = -30797;
  final public static int MDB_CORRUPTED = -30796;
  final public static int MDB_PANIC = -30795;
  final public static int MDB_VERSION_MISMATCH = -30794;
  final public static int MDB_INVALID = -30793;
  final public static int MDB_MAP_FULL = -30792;
  final public static int MDB_DBS_FULL = -30791;
  final public static int MDB_READERS_FULL = -30790;
  final public static int MDB_TLS_FULL = -30789;
  final public static int MDB_TXN_FULL = -30788;
  final public static int MDB_CURSOR_FULL = -30787;
  final public static int MDB_PAGE_FULL = -30786;
  final public static int MDB_MAP_RESIZED = -30785;
  final public static int MDB_INCOMPATIBLE = -30784;
  final public static int MDB_BAD_RSLOT = -30783;
  final public static int MDB_BAD_TXN = -30782;
  final public static int MDB_BAD_VALSIZE = -30781;
  final public static int MDB_BAD_DBI = -30780;
  
  /**
   * Handles mapping return codes to exceptions
   * 
   * @param name The name of the underlying API method we are calling
   * @param rc The return code from the message
   */
  public static void checkError(String name, int rc) {
    if (0 == rc) return;
    throw toException(name, rc);
  }
 
  /**
   * Handles mapping return codes to exceptions
   * 
   * @param name The name of the underlying API method we are calling
   * @param rc The return code from the message
   * @return the corresponding exception for the given rc code
   */
  public static LMDBException toException(String name, int rc) {
    if (0 == rc) throw new AssertionError("Cannot call this method with an rc of 0");
    
    final String message = name+" - "+Api.instance.mdb_strerror(rc);
    
    switch(rc) {
      case MDB_KEYEXIST:         return new KeyExists(rc, message);
      case MDB_NOTFOUND:         return new NotFound(rc, message);
      case MDB_PAGE_NOTFOUND:    return new PageNotFound(rc, message);
      case MDB_CORRUPTED:        return new Corrupted(rc, message);
      case MDB_PANIC:            return new Panic(rc, message);
      case MDB_VERSION_MISMATCH: return new VersionMismatch(rc, message);
      case MDB_INVALID:          return new Invalid(rc, message);
      case MDB_MAP_FULL:         return new MapFull(rc, message);
      case MDB_DBS_FULL:         return new DBSFull(rc, message);
      case MDB_READERS_FULL:     return new ReadersFull(rc, message);
      case MDB_TLS_FULL:         return new TLSFull(rc, message);
      case MDB_TXN_FULL:         return new TxnFull(rc, message);
      case MDB_CURSOR_FULL:      return new CursorFull(rc, message);
      case MDB_PAGE_FULL:        return new PageFull(rc, message);
      case MDB_MAP_RESIZED:      return new MapResized(rc, message);
      case MDB_INCOMPATIBLE:     return new Incompatible(rc, message);
      case MDB_BAD_RSLOT:        return new BadRSlot(rc, message);
      case MDB_BAD_TXN:          return new BadTxn(rc, message);
      case MDB_BAD_VALSIZE:      return new BadValSize(rc, message);
      case MDB_BAD_DBI:          return new BadDBI(rc, message);
      default:                   return new LMDBException(rc, message);
    }
  }
}
