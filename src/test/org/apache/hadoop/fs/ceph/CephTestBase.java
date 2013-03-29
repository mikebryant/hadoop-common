package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

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

  /*
   * Adapted test from HDFS test suite
   */
  public void testFileCreationNonRecursive() throws IOException {
    final Path path = new Path("/" + System.currentTimeMillis()
        + "-testFileCreationNonRecursive");
    FSDataOutputStream out = null;

    IOException expectedException = null;
    final String nonExistDir = "/non-exist-" + System.currentTimeMillis();

    fs.delete(new Path(nonExistDir), true);
    // Create a new file in root dir, should succeed
    out = createNonRecursive(fs, path, 1, false);
    out.close();
    // Create a file when parent dir exists as file, should fail
    expectedException = null;
    try {
      createNonRecursive(fs, new Path(path, "Create"), 1, false);
    } catch (IOException e) {
      expectedException = e;
    }
    assertTrue("Create a file when parent directory exists as a file"
        + " should throw FileAlreadyExistsException ",
        expectedException != null
        && expectedException instanceof FileAlreadyExistsException);
    fs.delete(path, true);
    // Create a file in a non-exist directory, should fail
    final Path path2 = new Path(nonExistDir + "/testCreateNonRecursive");
    expectedException = null;
    try {
      createNonRecursive(fs, path2, 1, false);
    } catch (IOException e) {
      expectedException = e;
    }
    assertTrue("Create a file in a non-exist dir using"
        + " createNonRecursive() should throw FileNotFoundException ",
        expectedException != null
        && expectedException instanceof FileNotFoundException);

    // Overwrite a file in root dir, should succeed
    out = createNonRecursive(fs, path, 1, true);
    out.close();
    // Overwrite a file when parent dir exists as file, should fail
    expectedException = null;
    try {
      createNonRecursive(fs, new Path(path, "Overwrite"), 1, true);
    } catch (IOException e) {
      expectedException = e;
    }
    assertTrue("Overwrite a file when parent directory exists as a file"
        + " should throw FileAlreadyExistsException ",
        expectedException != null
        && expectedException instanceof FileAlreadyExistsException);
    fs.delete(path, true);
    // Overwrite a file in a non-exist directory, should fail
    final Path path3 = new Path(nonExistDir + "/testOverwriteNonRecursive");
    expectedException = null;
    try {
      createNonRecursive(fs, path3, 1, true);
    } catch (IOException e) {
      expectedException = e;
    }
    assertTrue("Overwrite a file in a non-exist dir using"
        + " createNonRecursive() should throw FileNotFoundException ",
        expectedException != null
        && expectedException instanceof FileNotFoundException);
  }

  static FSDataOutputStream createNonRecursive(FileSystem fs, Path name,
      int repl, boolean overwrite) throws IOException {
    System.out.println("createNonRecursive: Created " + name + " with " + repl
        + " replica.");
    FSDataOutputStream stm = fs.createNonRecursive(
        name, FsPermission.getDefault(), overwrite, fs.getConf().getInt(
            "io.file.buffer.size", 4096), (short) repl, 8192, null);

    return stm;
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
