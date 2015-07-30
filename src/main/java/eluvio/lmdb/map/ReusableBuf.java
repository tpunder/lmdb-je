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

import java.nio.ByteBuffer;

/**
 * Meant to be used by a single thread.  Not Thread Safe.
 */
final class ReusableBuf implements AutoCloseable {
  public final ByteBuffer buf;
  private boolean inUse = false;
  
  public ReusableBuf(ByteBuffer buf) {
    this.buf = buf;
  }
  
  public void open() {
    if (inUse) throw new AssertionError("ReusableBufAlready in use!");
    inUse = true;
  }
  
  public void close() {
    if (null != buf) buf.clear();
    inUse = false;
  }
}