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
