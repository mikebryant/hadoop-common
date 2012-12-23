// -*- mode:Java; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*- 

/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * 
 * Wraps a number of native function calls to communicate with the Ceph
 * filesystem.
 */
package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.net.URI;
import java.io.FileNotFoundException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.lang.StringUtils;

import com.ceph.fs.CephMount;
import com.ceph.fs.CephStat;
import com.ceph.fs.CephFileAlreadyExistsException;
import com.ceph.fs.CephNotDirectoryException;

class CephTalker extends CephFS {

  private CephMount mount;
  private short defaultReplication;

  public CephTalker(Configuration conf, Log log) {
    mount = null;
  }

  private String pathString(Path path) {
    return path.toUri().getPath();
  }

  void initialize(URI uri, Configuration conf) throws IOException {
    mount = new CephMount("admin");

    /*
     * Load a configuration file if specified
     */
    String configfile = conf.get(
        CephConfigKeys.CEPH_CONF_FILE_KEY,
        CephConfigKeys.CEPH_CONF_FILE_DEFAULT);
    if (configfile != null) {
      mount.conf_read_file(configfile);
    }

    /*
     * Parse and set Ceph configuration options
     */
    String configopts = conf.get(
        CephConfigKeys.CEPH_CONF_OPTS_KEY,
        CephConfigKeys.CEPH_CONF_OPTS_DEFAULT);
    if (configopts != null) {
      String[] options = configopts.split(",");
      for (String option : options) {
          String[] keyval = option.split("=");
          if (keyval.length != 2) {
              throw new IllegalArgumentException("Invalid Ceph option: " + option);
          }
          String key = keyval[0];
          String val = keyval[1];
          try {
            mount.conf_set(key, val);
          } catch (Exception e) {
            throw new IOException("Error setting Ceph option " + key + " = " + val);
          }
      }
    }

    /*
     * Get default replication from configuration.
     */
    defaultReplication = (short)conf.getInt(
        CephConfigKeys.CEPH_REPLICATION_KEY,
        CephConfigKeys.CEPH_REPLICATION_DEFAULT);

    /*
     * Use a different root?
     */
    String root = conf.get(
        CephConfigKeys.CEPH_ROOT_DIR_KEY,
        CephConfigKeys.CEPH_ROOT_DIR_DEFAULT);

    /* Actually mount the file system */
    mount.mount(root);

    mount.chdir("/");
  }

  /*
   * Open a file. Ceph will not complain if we open a directory, but this
   * isn't something that Hadoop expects and we should throw an exception in
   * this case.
   */
  int open(Path path, int flags, int mode) throws IOException {
    int fd = mount.open(pathString(path), flags, mode);
    CephStat stat = new CephStat();
    fstat(fd, stat);
    if (stat.isDir()) {
      mount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }

  /*
   * Same as open(path, flags, mode) alternative, but takes custom striping
   * parameters that are used when a file is being created.
   */
  int open(Path path, int flags, int mode, int stripe_unit, int stripe_count,
      int object_size, String data_pool) throws IOException {
    int fd = mount.open(pathString(path), flags, mode, stripe_unit,
        stripe_count, object_size, data_pool);
    CephStat stat = new CephStat();
    fstat(fd, stat);
    if (stat.isDir()) {
      mount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }


  void fstat(int fd, CephStat stat) throws IOException {
    mount.fstat(fd, stat);
  }

  void lstat(Path path, CephStat stat) throws IOException {
    try {
      mount.lstat(pathString(path), stat);
    } catch (CephNotDirectoryException e) {
      throw new FileNotFoundException();
    }
  }

  void rmdir(Path path) throws IOException {
    mount.rmdir(pathString(path));
  }

  void unlink(Path path) throws IOException {
    mount.unlink(pathString(path));
  }

  void rename(Path src, Path dst) throws IOException {
    mount.rename(pathString(src), pathString(dst));
  }

  String[] listdir(Path path) throws IOException {
    CephStat stat = new CephStat();
    try {
      mount.lstat(pathString(path), stat);
    } catch (FileNotFoundException e) {
      return null;
    }
    if (!stat.isDir())
      return null;
    return mount.listdir(pathString(path));
  }

  void mkdirs(Path path, int mode) throws IOException {
    mount.mkdirs(pathString(path), mode);
  }

  void close(int fd) throws IOException {
    mount.close(fd);
  }

  void chmod(Path path, int mode) throws IOException {
    mount.chmod(pathString(path), mode);
  }

  void shutdown() throws IOException {
    mount.unmount();
    mount = null;
  }

  short getDefaultReplication() {
    return defaultReplication;
  }

  short get_file_replication(Path path) throws IOException {
    CephStat stat = new CephStat();
    mount.lstat(pathString(path), stat);
    int replication = 1;
    if (stat.isFile()) {
      int fd = mount.open(pathString(path), CephMount.O_RDONLY, 0);
      replication = mount.get_file_replication(fd);
      mount.close(fd);
    }
    return (short)replication;
  }

  void setattr(Path path, CephStat stat, int mask) throws IOException {
    mount.setattr(pathString(path), stat, mask);
  }

  long lseek(int fd, long offset, int whence) throws IOException {
    return mount.lseek(fd, offset, whence);
  }

  int write(int fd, byte[] buf, long size, long offset) throws IOException {
    return (int)mount.write(fd, buf, size, offset);
  }

  int read(int fd, byte[] buf, long size, long offset) throws IOException {
    return (int)mount.read(fd, buf, size, offset);
  }

}
