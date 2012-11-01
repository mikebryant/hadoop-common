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
 * This uses the local Filesystem but pretends to be communicating
 * with a Ceph deployment, for unit testing the CephFileSystem.
 */

package org.apache.hadoop.fs.ceph;


import java.net.URI;
import java.util.Hashtable;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import com.ceph.fs.CephMount;
import com.ceph.fs.CephStat;

class CephFaker extends CephFS {
  private static final Log LOG = LogFactory.getLog(CephFaker.class);
  FileSystem localFS;
  String localPrefix;
  Configuration conf;
  Hashtable<Integer, Object> files;
  Hashtable<Integer, String> filenames;
  int fileCount = 0;
  boolean initialized = false;
	
  public CephFaker(Configuration con, Log log) {
    conf = con;
    files = new Hashtable<Integer, Object>();
    filenames = new Hashtable<Integer, String>();
  }
	
  void initialize(URI uri, Configuration conf) throws IOException {
    if (!initialized) {
      /* for a real Ceph deployment, this starts up the client, 
       * sets debugging levels, etc. We just need to get the
       * local FileSystem to use, and we'll ignore any
       * command-line arguments. */
      localFS = FileSystem.getLocal(conf);
      localFS.initialize(URI.create("file://localhost"), conf);
      localFS.setVerifyChecksum(false);
      String testDir = conf.get("hadoop.tmp.dir");

      localPrefix = localFS.getWorkingDirectory().toString();
      int testDirLoc = localPrefix.indexOf(testDir) - 1;

      if (-2 == testDirLoc) {
        testDirLoc = localPrefix.length();
      }
      localPrefix = localPrefix.substring(0, testDirLoc) + "/"
          + conf.get("hadoop.tmp.dir");

      localFS.setWorkingDirectory(
          new Path(localPrefix + "/user/" + System.getProperty("user.name")));
      // I don't know why, but the unit tests expect the default
      // working dir to be /user/username, so satisfy them!
      // debug("localPrefix is " + localPrefix, INFO);
      initialized = true;
    }
  }

  protected String ceph_getcwd() {
    return sanitize_path(localFS.getWorkingDirectory().toString());
  }

  protected boolean ceph_setcwd(String path) {
    localFS.setWorkingDirectory(new Path(prepare_path(path)));
    return true;
  }

  // the caller is responsible for ensuring empty dirs
  void rmdir(Path pth) throws IOException {
    Path path = new Path(prepare_path(pth.toUri().getPath()));
    if (localFS.listStatus(path).length <= 1) {
      localFS.delete(path, true);
    }
  }

  // this needs to work on (empty) directories too
  void unlink(Path path) throws IOException {
    String pathStr = prepare_path(path.toUri().getPath());
    localFS.delete(new Path(pathStr), false);
  }

  protected boolean ceph_rename(String oldName, String newName) {
    oldName = prepare_path(oldName);
    newName = prepare_path(newName);
    try {
      Path parent = new Path(newName).getParent();
      Path newPath = new Path(newName);

      if (localFS.exists(parent) && !localFS.exists(newPath)) {
        return localFS.rename(new Path(oldName), newPath);
      }
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  protected long ceph_getblocksize(String path) {
    path = prepare_path(path);
    try {
      FileStatus status = localFS.getFileStatus(new Path(path));

      return status.getBlockSize();
    } catch (FileNotFoundException e) {
      return -CephFS.ENOENT;
    } catch (IOException e) {
      return -1; // just fail generically
    }
  }

  String[] listdir(Path pth) throws IOException {
    String path = prepare_path(pth.toUri().getPath());
    try {
      FileStatus[] stats = localFS.listStatus(new Path(path));
      String[] names = new String[stats.length];
      String name;

      for (int i = 0; i < stats.length; ++i) {
        name = stats[i].getPath().toString();
        names[i] = name.substring(name.lastIndexOf(Path.SEPARATOR) + 1);
      }
      return names;
    } catch (IOException e) {}
    return null;
  }

  protected int ceph_mkdirs(String path, int mode) throws IOException {
    path = prepare_path(path);
    localFS.mkdirs(new Path(path), new FsPermission((short) mode));
    return 0;
  }

  protected int open(Path path, int flags, int mode) {
    String pathStr = prepare_path(path.toUri().getPath());

    int appendFlags = CephMount.O_WRONLY|CephMount.O_CREAT|CephMount.O_APPEND;
    int overwriteFlags = CephMount.O_WRONLY|CephMount.O_CREAT|CephMount.O_TRUNC;

    try {
      if (flags == appendFlags) {
        FSDataOutputStream stream = localFS.append(new Path(pathStr));
        files.put(new Integer(fileCount), stream);
      } else if (flags == overwriteFlags) {
        FSDataOutputStream stream = localFS.create(new Path(pathStr));
        files.put(new Integer(fileCount), stream);
      } else {
        FSDataInputStream stream = localFS.open(new Path(pathStr));
        files.put(new Integer(fileCount), stream);
      }
      filenames.put(new Integer(fileCount), pathStr);
      LOG.info("ceph_open_for_read fh:" + fileCount + ", pathname:" + pathStr);
      return fileCount++;
    } catch (IOException e) {}
    return -1; // failure
  }

  protected int ceph_close(int filehandle) {
    LOG.info("ceph_close(filehandle " + filehandle + ")");
    try {
      ((Closeable) files.get(new Integer(filehandle))).close();
      if (null == files.get(new Integer(filehandle))) {
        return -ENOENT; // this isn't quite the right error code,
        // but the important part is it's negative
      }
      return 0; // hurray, success
    } catch (NullPointerException ne) {
      LOG.warn("ceph_close caught NullPointerException!" + ne);
    } // err, how?
    catch (IOException ie) {
      LOG.warn("ceph_close caught IOException!" + ie);
    }
    return -1; // failure
  }

  void chmod(Path pth, int mode) throws IOException {
    String path = prepare_path(pth.toUri().getPath());
    localFS.setPermission(new Path(path), new FsPermission((short) mode));
  }

  // rather than try and match a Ceph deployment's behavior exactly,
  // just make bad things happen if they try and call methods after this
  protected boolean ceph_kill_client() {
    // debug("ceph_kill_client", INFO);
    localFS.setWorkingDirectory(new Path(localPrefix));
    // debug("working dir is now " + localFS.getWorkingDirectory(), INFO);
    try {
      localFS.close();
    } catch (Exception e) {}
    localFS = null;
    files = null;
    filenames = null;
    return true;
  }

  private void stat(String path, CephStat fill) throws IOException {
    FileStatus status = localFS.getFileStatus(new Path(path));
    fill.size = status.getLen();
    fill.is_directory = status.isDir();
    fill.blksize = status.getBlockSize();
    fill.m_time = status.getModificationTime();
    fill.a_time = status.getAccessTime();
    fill.mode = status.getPermission().toShort();
  }

  void fstat(int fd, CephStat stat) throws IOException {
    String path = filenames.get(new Integer(fd));
    stat(path, stat);
  }

  void lstat(Path pth, CephStat fill) throws IOException {
    String path = prepare_path(pth.toUri().getPath());
    stat(path, fill);
  }

  protected int ceph_replication(Path pth) {
    String path = prepare_path(pth.toUri().getPath());
    int ret = -1; // -1 for failure

    try {
      ret = localFS.getFileStatus(new Path(path)).getReplication();
    } catch (IOException e) {}
    return ret;
  }

  protected String[] ceph_hosts(int fh, long offset) {
    String[] ret = null;

    try {
      BlockLocation[] locs = localFS.getFileBlockLocations(
          localFS.getFileStatus(new Path(filenames.get(new Integer(fh)))),
          offset, 1);

      ret = locs[0].getNames();
    } catch (IOException e) {} catch (NullPointerException f) {}
    return ret;
  }

  void setattr(Path pth, CephStat stat, int mask) throws IOException {
    String path = prepare_path(pth.toUri().getPath());
    long mtime = -1;
    long atime = -1;
    if ((mask & CephMount.SETATTR_MTIME) != 0) {
      mtime = stat.m_time;
    }
    if ((mask & CephMount.SETATTR_ATIME) != 0) {
      atime = stat.a_time;
    }
    localFS.setTimes(new Path(path), mtime, atime);
  }

  private long ceph_getpos(int fh) {
    long ret = -1; // generic fail

    try {
      Object stream = files.get(new Integer(fh));

      if (stream instanceof FSDataInputStream) {
        ret = ((FSDataInputStream) stream).getPos();
      } else if (stream instanceof FSDataOutputStream) {
        ret = ((FSDataOutputStream) stream).getPos();
      }
    } catch (IOException e) {} catch (NullPointerException f) {}
    return ret;
  }

  protected int ceph_write(int fh, byte[] buffer,
      int buffer_offset, int length) {
    LOG.info(
        "ceph_write fh:" + fh + ", buffer_offset:" + buffer_offset + ", length:"
        + length);
    long ret = -1; // generic fail

    try {
      FSDataOutputStream os = (FSDataOutputStream) files.get(new Integer(fh));

      LOG.info("ceph_write got outputstream");
      long startPos = os.getPos();

      os.write(buffer, buffer_offset, length);
      ret = os.getPos() - startPos;
    } catch (IOException e) {
      LOG.warn("ceph_write caught IOException!");
    } catch (NullPointerException f) {
      LOG.warn("ceph_write caught NullPointerException!");
    }
    return (int) ret;
  }

  protected int ceph_read(int fh, byte[] buffer,
      int buffer_offset, int length) {
    long ret = -1; // generic fail

    try {
      FSDataInputStream is = (FSDataInputStream) files.get(new Integer(fh));
      long startPos = is.getPos();

      is.read(buffer, buffer_offset, length);
      ret = is.getPos() - startPos;
    } catch (IOException e) {} catch (NullPointerException f) {}
    return (int) ret;
  }

  long lseek(int fd, long pos, int whence) throws IOException {
    if (whence == CephMount.SEEK_SET) {
      return ceph_seek_from_start(fd, pos);
    } else if (whence == CephMount.SEEK_CUR && pos == 0) {
      return ceph_getpos(fd);
    } else {
      return -1;
    }
  }

  private long ceph_seek_from_start(int fh, long pos) {
    LOG.info("ceph_seek_from_start(fh " + fh + ", pos " + pos + ")");
    long ret = -1; // generic fail

    try {
      LOG.info("ceph_seek_from_start filename is " + filenames.get(new Integer(fh)));
      if (null == files.get(new Integer(fh))) {
        LOG.warn("ceph_seek_from_start: is is null!");
      }
      FSDataInputStream is = (FSDataInputStream) files.get(new Integer(fh));

      LOG.info("ceph_seek_from_start retrieved is!");
      is.seek(pos);
      ret = is.getPos();
    } catch (IOException e) {
      LOG.warn("ceph_seek_from_start caught IOException!");
    } catch (NullPointerException f) {
      LOG.warn("ceph_seek_from_start caught NullPointerException!");
    }
    return (int) ret;
  }

  /*
   * We need to remove the localFS file prefix before returning to Ceph
   */
  private String sanitize_path(String path) {
    // debug("sanitize_path(" + path + ")", INFO);
    /* if (path.startsWith("file:"))
     path = path.substring("file:".length()); */
    if (path.startsWith(localPrefix)) {
      path = path.substring(localPrefix.length());
      if (path.length() == 0) { // it was a root path
        path = "/";
      }
    }
    // debug("sanitize_path returning " + path, INFO);
    return path;
  }

  /*
   * If it's an absolute path we need to shove the
   * test dir onto the front as a prefix.
   */
  private String prepare_path(String path) {
    // debug("prepare_path(" + path + ")", INFO);
    if (path.startsWith("/")) {
      path = localPrefix + path;
    } else if (path.equals("..")) {
      if (ceph_getcwd().equals("/")) {
        path = ".";
      } // you can't go up past root!
    }
    // debug("prepare_path returning" + path, INFO);
    return path;
  }
}
