package eluvio.lmdb.map;

public class LMDBOutOfRangeException extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;

  public LMDBOutOfRangeException(String msg) {
    super(msg);
  }
}
