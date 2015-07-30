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

public class Stat {
  public final long psize;
  public final long depth;
  public final long branchPages;
  public final long leafPages;
  public final long overflowPages;
  public final long entries;
  
  protected Stat(Api.MDB_stat stat) {
    psize = stat.ms_psize.longValue();
    depth = stat.ms_depth.longValue();
    branchPages = stat.ms_branch_pages.longValue();
    leafPages = stat.ms_leaf_pages.longValue();
    overflowPages = stat.ms_overflow_pages.longValue();
    entries = stat.ms_entries.longValue();
  }
  
  public String toString() {
    return "Stat(psize: "+psize+", depth: "+depth+", branchPages: "+branchPages+", leafPages: "+leafPages+", overflowPages: "+overflowPages+", entries: "+entries+")";
  }
}
