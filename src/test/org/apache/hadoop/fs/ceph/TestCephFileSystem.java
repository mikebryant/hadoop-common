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
    conf.set("fs.ceph.conf.file", "/home/nwatkins/projects/ceph/ceph/src/ceph.conf");
    fs = new CephFileSystem();
    fs.initialize(uri, conf);
  }

}
