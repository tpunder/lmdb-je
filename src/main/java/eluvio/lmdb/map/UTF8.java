//package eluvio.lmdb.map;
//
//import java.nio.ByteBuffer;
//import java.nio.charset.StandardCharsets;
//
//class UTF8 {
//  // We are only supporting up to 3-byte UTF-8 characters
//  private static int MAX_UTF8_CHAR_BYTES = 3;
//  
//  public static ByteBuffer write(String s, ByteBuffer buf) {
//    final int maxSize = MAX_UTF8_CHAR_BYTES * s.length();
//    
//    if (null != buf && buf.remaining() >= maxSize) {
//      writeFast(s, buf);
//      buf.flip();
//      return buf;
//    }
//    
//    return writeSlow(s, buf);
//  }
//  
//  // Assumes non-null && non-zero-length String
//  private static void writeFast(String s, ByteBuffer buf) {
//    final int len = s.length();
//    
//    int i = 0;
//    char ch = s.charAt(0);
//    
//    // Fast path for writing ASCII chars
//    while (i < len && ch <= 0x7F) {
//      buf.put((byte)ch);
//      i++;
//      if (i < len) ch = s.charAt(i);
//    }
//    
//    // Slower path for writing any remaining chars which might be a mix of ASCII and multi-byte UTF-8
//    while (i < len) {
//      writeChar(s.charAt(i), buf);
//      i++;
//    }
//  }
//  
//  private static ByteBuffer writeSlow(String s, ByteBuffer buf) {
//    final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
//    
//    if (null == buf || bytes.length > buf.remaining()) buf = ByteBuffer.allocateDirect(bytes.length);
//    
//    buf.put(bytes);
//    buf.flip();
//    return buf;
//  }
//  
//  public static String read(ByteBuffer buf) {
//    final byte[] bytes = new byte[buf.remaining()];
//    buf.get(bytes);
//    return new String(bytes, StandardCharsets.UTF_8);
//  }
//  
//  public static int caseSensitiveCompare(ByteBuffer a, ByteBuffer b) {    
//    while (a.remaining() > 0 && b.remaining() > 0) {
//      char ch1 = readChar(a);
//      char ch2 = readChar(b);
//      
//      if (ch1 != ch2) return ch1 - ch2;
//    }
//    
//    return a.remaining() - b.remaining();
//  }
//  
//  public static int caseInsensitiveCompare(ByteBuffer a, ByteBuffer b) {
//    while (a.remaining() > 0 && b.remaining() > 0) {
//      char ch1 = readChar(a);
//      char ch2 = readChar(b);
//      
//      if (ch1 != ch2) {
//        ch1 = Character.toUpperCase(ch1);
//        ch2 = Character.toUpperCase(ch2);
//        if (ch1 != ch2) {
//          ch1 = Character.toLowerCase(ch1);
//          ch2 = Character.toLowerCase(ch2);
//          if (ch1 != ch2) {
//            return ch1 - ch2;
//          }
//        }
//      }
//    }
//    
//    return a.remaining() - b.remaining();
//  }
//  
//  private static char readChar(ByteBuffer buf) {
//    byte b = buf.get();
//    
//    if (b <= 0x7F) return (char)b;
//    
//    throw new UnsupportedOperationException("???");
//  }
//  
////  private static void writeChar(char ch, ByteBuffer buf) {
////    if (ch <= 0x007F) {
////      buf.put((byte)ch);
////    } else if (ch <= 0x07FF) {
////      buf.put((byte)(0xC0 | ((ch >> 6) & 0x1F)));
////      buf.put((byte)(0x80 | ((ch     ) & 0x3F)));
////    } else if (ch <= 0xFFFF) {
////      buf.put((byte)(0xE0 | ((ch >> 12) & 0x0F)));
////      buf.put((byte)(0x80 | ((ch >>  6) & 0x3F)));
////      buf.put((byte)(0x80 | ((ch      ) & 0x3F)));
////    } else {
////      throw new IllegalArgumentException("Can't write 4-byte char: "+ch);
////    }
////  }
//    
//  private static void writeChar(char ch, ByteBuffer buf) {
//    if (ch <= 0x007F) write1ByteChar(ch, buf);
//    else if (ch <= 0x07FF) write2ByteChar(ch, buf);
//    else if (ch <= 0xFFFF) write3ByteChar(ch, buf);
//    else write4ByteChar(ch, buf);
//  }
//  
//  private static void write1ByteChar(char ch, ByteBuffer buf) {
//    buf.put((byte)ch);
//  }
//  
//  private static void write2ByteChar(char ch, ByteBuffer buf) {
//    buf.put((byte)(0xC0 | ((ch >> 6) & 0x1F)));
//    buf.put((byte)(0x80 | ((ch     ) & 0x3F)));
//  }
//  
//  private static void write3ByteChar(char ch, ByteBuffer buf) {
//    buf.put((byte)(0xE0 | ((ch >> 12) & 0x0F)));
//    buf.put((byte)(0x80 | ((ch >>  6) & 0x3F)));
//    buf.put((byte)(0x80 | ((ch      ) & 0x3F)));
//  }
//  
//  private static void write4ByteChar(char ch, ByteBuffer buf) {
//    throw new IllegalArgumentException("Can't write 4-byte char: "+ch);
//  }
//}
