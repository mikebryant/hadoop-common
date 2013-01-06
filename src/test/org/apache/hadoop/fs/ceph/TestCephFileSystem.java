package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestCephFileSystem extends FileSystemContractBaseTest {

  @Override
  protected void setUp() throws IOException {
    Configuration conf = new Configuration();
    URI uri = URI.create("ceph:///");
    //pull the path to the conf file out of the environment
    String cephConfFile = System.getProperty(CephConfigKeys.CEPH_CONF_FILE_KEY);

    if (cephConfFile != null) {
      conf.set(CephConfigKeys.CEPH_CONF_FILE_KEY, cephConfFile);
    }

    fs = new CephFileSystem();
    fs.initialize(uri, conf);
  }

}
