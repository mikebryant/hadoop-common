package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class TestCephDefaultReplication extends CephTestBase {
  /*
   * Expecting no data pools to be specified
   */
  public void testDefaultReplication() throws Exception {
    String pools[] = getConfiguredDataPools();
    assertTrue(pools.length == 0);

    /* create file with default replication */
    Path path = new Path("/file.def.repl");
    createFile(path);

    /*
     * Hard-coded default pool name for now. We don't have easy access to the
     * raw Ceph FS interface from here. The structure of the Hadoop binding
     * will be changed, and we should allow test access to the low level
     * interface.
     */
    int default_repl = getPoolReplication("data");

    /* Check file replication is default factor */
    FileStatus status = fs.getFileStatus(path);
    assertTrue(status.getReplication() == default_repl);
  }
}
