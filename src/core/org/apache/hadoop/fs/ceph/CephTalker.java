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

class CephTalker extends CephFS {

  private CephMount mount;

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
    String configfile = conf.get("fs.ceph.conf.file", null);
    if (configfile != null) {
      mount.conf_read_file(configfile);
    }

    /*
     * Parse and set Ceph configuration options
     */
    String configopts = conf.get("fs.ceph.conf.options", null);
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

    /* Passing root = null to mount() will default to "/" */
    String root = StringUtils.stripToNull(uri.getPath());
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
    if (stat.is_directory) {
      mount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }

  void fstat(int fd, CephStat stat) throws IOException {
    mount.fstat(fd, stat);
  }

  protected String ceph_getcwd() throws IOException {
    return mount.getcwd();
  }

  protected boolean ceph_setcwd(String path) throws IOException {
    mount.chdir(path);
    return true;
  }

  void rmdir(Path path) throws IOException {
    mount.rmdir(pathString(path));
  }

  void unlink(Path path) throws IOException {
    mount.unlink(pathString(path));
  }

  protected boolean ceph_rename(String old_path, String new_path) throws IOException {
    mount.rename(old_path, new_path);
    return true;
  }

  protected long ceph_getblocksize(String path) throws IOException {
    int fd = mount.open(path, CephMount.O_RDONLY, 0);
    int block_size = mount.get_file_stripe_unit(fd);
    mount.close(fd);
    return (long)block_size;
  }

  protected String[] ceph_getdir(String path) throws IOException {
    CephStat stat = new CephStat();
    try {
      mount.lstat(path, stat);
    } catch (FileNotFoundException e) {
      return null;
    }
    if (stat.is_file)
      return null;
    return mount.listdir(path);
  }

  protected int ceph_mkdirs(String path, int mode) throws IOException {
    try {
      mount.mkdirs(path, mode);
    } catch (CephFileAlreadyExistsException e) {
      return 1;
    }
    return 0;
  }

  protected native int ceph_open_for_append(String path);

  protected int ceph_open_for_overwrite(String path, int mode) throws IOException {
    int flags = CephMount.O_WRONLY|CephMount.O_CREAT|CephMount.O_TRUNC;
    return mount.open(path, flags, mode);
  }

  protected int ceph_close(int filehandle) throws IOException {
    mount.close(filehandle);
    return 0;
  }

  protected boolean ceph_setPermission(String path, int mode) throws IOException {
    mount.chmod(path, mode);
    return true;
  }

  protected boolean ceph_kill_client() throws IOException {
    mount.unmount();
    mount = null;
    return true;
  }

  protected boolean ceph_stat(String path, CephFileSystem.Stat fill) throws IOException {
    CephStat stat = new CephStat();
    try {
      mount.lstat(path, stat);
    } catch (FileNotFoundException e) {
      return false;
    }
    fill.size = stat.size;
    fill.is_dir = stat.is_directory;
    fill.block_size = stat.blksize;
    fill.mod_time = stat.m_time;
    fill.access_time = stat.a_time;
    fill.mode = stat.mode;
    return true;
  }

  protected int ceph_replication(String path) throws IOException {
    CephStat stat = new CephStat();
    mount.lstat(path, stat);
    int replication = 1;
    if (stat.is_file) {
      int fd = mount.open(path, 0, CephMount.O_RDONLY);
      replication = mount.get_file_replication(fd);
      mount.close(fd);
    }
    return replication;
  }

  protected String[] ceph_hosts(int fh, long offset) {
    return new String[] {};
  }

  protected native int ceph_setTimes(String path, long mtime, long atime);

  protected long ceph_getpos(int fh) throws IOException {
    return mount.lseek(fh, 0, CephMount.SEEK_CUR);
  }

  protected int ceph_write(int fh, byte[] buffer, int buffer_offset, int length) throws IOException {
    assert buffer_offset == 0;
    return (int)mount.write(fh, buffer, length, -1);
  }

  protected int ceph_read(int fh, byte[] buffer, int buffer_offset, int length) throws IOException {
    assert buffer_offset == 0;
    return (int)mount.read(fh, buffer, length, -1);
  }

  protected long ceph_seek_from_start(int fh, long pos) throws IOException {
    return mount.lseek(fh, pos, CephMount.SEEK_SET);
  }
}
