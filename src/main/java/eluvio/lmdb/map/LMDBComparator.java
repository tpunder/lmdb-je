package eluvio.lmdb.map;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface LMDBComparator {
  public int compare(ByteBuffer a, ByteBuffer b);
  
  public static CaseSensitiveStringComparator CASE_SENSITIVE_STRING_COMPARATOR = new CaseSensitiveStringComparator();
  
  public static CaseInsensitiveStringComparator CASE_INSENSITIVE_STRING_COMPARATOR = new CaseInsensitiveStringComparator();
  
  final public static class CaseSensitiveStringComparator implements Comparator<String>, LMDBComparator {
    @Override
    public int compare(String a, String b) {
      return a.compareTo(b);
    }
    
    @Override
    public int compare(ByteBuffer a, ByteBuffer b) {
      //return UTF8.caseSensitiveCompare(a, b);
      return compare(LMDBSerializer.String.deserialize(a), LMDBSerializer.String.deserialize(b));
    }
  }
  
  final public static class CaseInsensitiveStringComparator implements Comparator<String>, LMDBComparator {
    @Override
    public int compare(String a, String b) {
      return String.CASE_INSENSITIVE_ORDER.compare(a, b);
    }
    
    @Override
    public int compare(ByteBuffer a, ByteBuffer b) {
      //return UTF8.caseInsensitiveCompare(a, b);
      return compare(LMDBSerializer.String.deserialize(a), LMDBSerializer.String.deserialize(b));
    }
  }
}
