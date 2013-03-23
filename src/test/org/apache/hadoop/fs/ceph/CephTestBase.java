package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class CephTestBase extends FileSystemContractBaseTest {

  @Override
  protected void setUp() throws IOException {
    Configuration conf = new Configuration();

    String conf_file = System.getProperty("hadoop.conf.file");
    conf.addResource(new Path(conf_file));

    URI uri = FileSystem.getDefaultUri(conf);

    fs = new CephFileSystem();
    fs.initialize(uri, conf);
  }

  protected String[] getConfiguredDataPools() throws Exception {
    CephFileSystem cephfs = (CephFileSystem)fs;
    return cephfs.getConfiguredDataPools();
  }

  protected int getPoolReplication(String name) throws Exception {
    CephFileSystem cephfs = (CephFileSystem)fs;
    return cephfs.getPoolReplication(name);
  }
}
