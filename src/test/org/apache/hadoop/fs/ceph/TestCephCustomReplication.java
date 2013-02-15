package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

public class TestCephCustomReplication extends CephTestBase {

  public void testCustomReplication() throws Exception {
    String pools[] = getConfiguredDataPools();
    assertTrue(pools.length > 0);

    int i = 0;
    for (String pool : pools) {
      int repl = getPoolReplication(pool);

      Path path = new Path("/file.custom.repl." + pool + "." + repl);
      System.out.println("path " + path + " pool " + pool + " repl " + repl);

      FSDataOutputStream out = fs.create(path, false, 4096,
          (short)repl, fs.getDefaultBlockSize());
      out.write(data, 0, data.length);
      out.close();

      FileStatus status = fs.getFileStatus(path);
      assertTrue(status.getReplication() == repl);

      fs.delete(path);
    }
  }
}
