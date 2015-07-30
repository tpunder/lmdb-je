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

public class LMDBException extends RuntimeException {  
  private static final long serialVersionUID = 1L;

  public final int rc;
  
  protected LMDBException(int rc, String message) {
    super(message);
    this.rc = rc;
  }
  
  public final static class KeyExists extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected KeyExists(int rc, String message){ super(rc, message); }
  }
  
  public final static class NotFound extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected NotFound(int rc, String message){ super(rc, message); }
  }
  
  public final static class PageNotFound extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected PageNotFound(int rc, String message){ super(rc, message); }
  }
  
  public final static class Corrupted extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected Corrupted(int rc, String message){ super(rc, message); }
  }
  
  public final static class Panic extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected Panic(int rc, String message){ super(rc, message); }
  }
  
  public final static class VersionMismatch extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected VersionMismatch(int rc, String message){ super(rc, message); }
  }
  
  public final static class Invalid extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected Invalid(int rc, String message){ super(rc, message); }
  }
  
  public final static class MapFull extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected MapFull(int rc, String message){ super(rc, message); }
  }
  
  public final static class DBSFull extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected DBSFull(int rc, String message){ super(rc, message); }
  }
  
  public final static class ReadersFull extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected ReadersFull(int rc, String message){ super(rc, message); }
  }
  
  public final static class TLSFull extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected TLSFull(int rc, String message){ super(rc, message); }
  }
  
  public final static class TxnFull extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected TxnFull(int rc, String message){ super(rc, message); }
  }
  
  public final static class CursorFull extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected CursorFull(int rc, String message){ super(rc, message); }
  }
  
  public final static class PageFull extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected PageFull(int rc, String message){ super(rc, message); }
  }
  
  public final static class MapResized extends LMDBException {
    private static final long serialVersionUID = 1L;
    MapResized(int rc, String message){ super(rc, message); }
  }
  
  public final static class Incompatible extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected Incompatible(int rc, String message){ super(rc, message); }
  }
  
  public final static class BadRSlot extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected BadRSlot(int rc, String message){ super(rc, message); }
  }
  
  public final static class BadTxn extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected BadTxn(int rc, String message){ super(rc, message); }
  }
  
  public final static class BadValSize extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected BadValSize(int rc, String message){ super(rc, message); }
  }
  
  public final static class BadDBI extends LMDBException {
    private static final long serialVersionUID = 1L;
    protected BadDBI(int rc, String message){ super(rc, message); }
  }
}
