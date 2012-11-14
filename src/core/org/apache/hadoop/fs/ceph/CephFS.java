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
 * Abstract base class for communicating with a Ceph filesystem and its
 * C++ codebase from Java, or pretending to do so (for unit testing purposes).
 * As only the Ceph package should be using this directly, all methods
 * are protected.
 */
package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import com.ceph.fs.CephStat;

abstract class CephFS {

  protected static final int ENOTDIR = 20;
  protected static final int EEXIST = 17;
  protected static final int ENOENT = 2;

  abstract void initialize(URI uri, Configuration conf) throws IOException;
  abstract int open(Path path, int flags, int mode) throws IOException;
  abstract void fstat(int fd, CephStat stat) throws IOException;
  abstract void lstat(Path path, CephStat stat) throws IOException;
  abstract void unlink(Path path) throws IOException;
  abstract void rmdir(Path path) throws IOException;
  abstract String[] listdir(Path path) throws IOException;
  abstract void setattr(Path path, CephStat stat, int mask) throws IOException;
  abstract void chmod(Path path, int mode) throws IOException;
  abstract long lseek(int fd, long offset, int whence) throws IOException;
  abstract void close(int fd) throws IOException;
  abstract void shutdown() throws IOException;
  abstract void rename(Path src, Path dst) throws IOException;

  /*
   * Returns the current working directory (absolute) as a String
   */
  abstract protected String ceph_getcwd() throws IOException;

  /*
   * Changes the working directory.
   * Inputs:
   *  String path: The path (relative or absolute) to switch to
   * Returns: true on success, false otherwise.
   */
  abstract protected boolean ceph_setcwd(String path) throws IOException;

  /*
   * Get the block size for a given path.
   * Input:
   *  String path: The path (relative or absolute) you want
   *  the block size for.
   * Returns: block size if the path exists, otherwise a negative number
   *  corresponding to the standard C++ error codes (which are positive).
   */
  abstract protected long ceph_getblocksize(String path) throws IOException;

  /*
   * Create the specified directory and any required intermediate ones with the
   * given mode.
   */
  abstract protected int ceph_mkdirs(String path, int mode) throws IOException;

  /*
   * Check how many times a file should be replicated. If it is,
   * degraded it may not actually be replicated this often.
   * Inputs:
   *  int fh: a file descriptor
   * Returns: an int containing the number of times replicated.
   */
  abstract protected int ceph_replication(Path path) throws IOException;

  /*
   * Write the given buffer contents to the given filehandle.
   * Inputs:
   *  int fh: The filehandle to write to.
   *  byte[] buffer: The buffer to write from
   *  int buffer_offset: The position in the buffer to write from
   *  int length: The number of (sequential) bytes to write.
   * Returns: int, on success the number of bytes written, on failure
   *  a negative error code.
   */
  abstract protected int ceph_write(int fh, byte[] buffer, int buffer_offset, int length) throws IOException;

  /*
   * Reads into the given byte array from the current position.
   * Inputs:
   *  int fh: the filehandle to read from
   *  byte[] buffer: the byte array to read into
   *  int buffer_offset: where in the buffer to start writing
   *  int length: how much to read.
   * There'd better be enough space in the buffer to write all
   * the data from the given offset!
   * Returns: the number of bytes read on success (as an int),
   *  or an error code otherwise.	 */
  abstract protected int ceph_read(int fh, byte[] buffer, int buffer_offset, int length) throws IOException;

}
