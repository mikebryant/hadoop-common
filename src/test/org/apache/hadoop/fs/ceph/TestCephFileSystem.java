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

    String conf_file = System.getProperty("hadoop.conf.file");
    conf.addResource(new Path(conf_file));

    URI uri = URI.create("ceph:///");

    fs = new CephFileSystem();
    fs.initialize(uri, conf);
  }

}
