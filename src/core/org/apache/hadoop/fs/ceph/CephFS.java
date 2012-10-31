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
  abstract void unlink(Path path) throws IOException;
  abstract void rmdir(Path path) throws IOException;

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
   * Changes a given path name to a new name, assuming new_path doesn't exist.
   * Inputs:
   *  jstring j_from: The path whose name you want to change.
   *  jstring j_to: The new name for the path.
   * Returns: true if the rename occurred, false otherwise
   */
  abstract protected boolean ceph_rename(String old_path, String new_path) throws IOException;

  /*
   * Returns true if it the input path exists, false
   * if it does not or there is an unexpected failure.
   */
  abstract protected boolean ceph_exists(String path) throws IOException;

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
   * Returns true if the given path is a directory, false otherwise.
   */
  abstract protected boolean ceph_isdirectory(String path) throws IOException;

  /*
   * Returns true if the given path is a file; false otherwise.
   */
  abstract protected boolean ceph_isfile(String path) throws IOException;

  /*
   * Get the contents of a given directory.
   * Inputs:
   *  String path: The path (relative or absolute) to the directory.
   * Returns: A Java String[] of the contents of the directory, or
   *  NULL if there is an error (ie, path is not a dir). This listing
   *  will not contain . or .. entries.
   */
  abstract protected String[] ceph_getdir(String path) throws IOException;

  /*
   * Create the specified directory and any required intermediate ones with the
   * given mode.
   */
  abstract protected int ceph_mkdirs(String path, int mode) throws IOException;

  /*
   * Open a file to append. If the file does not exist, it will be created.
   * Opening a dir is possible but may have bad results.
   * Inputs:
   *  String path: The path to open.
   * Returns: an int filehandle, or a number<0 if an error occurs.
   */
  abstract protected int ceph_open_for_append(String path);

  /*
   * Opens a file for overwriting; creates it if necessary.
   * Opening a dir is possible but may have bad results.
   * Inputs:
   *  String path: The path to open.
   *  int mode: The mode to open with.
   * Returns: an int filehandle, or a number<0 if an error occurs.
   */
  abstract protected int ceph_open_for_overwrite(String path, int mode) throws IOException;

  /*
   * Closes the given file. Returns 0 on success, or a negative
   * error code otherwise.
   */
  abstract protected int ceph_close(int filehandle) throws IOException;

  /*
   * Change the mode on a path.
   * Inputs:
   *  String path: The path to change mode on.
   *  int mode: The mode to apply.
   * Returns: true if the mode is properly applied, false if there
   *  is any error.
   */
  abstract protected boolean ceph_setPermission(String path, int mode) throws IOException;

  /*
   * Closes the Ceph client. This should be called before shutting down
   * (multiple times is okay but redundant).
   */
  abstract protected boolean ceph_kill_client() throws IOException;

  /*
   * Get the statistics on a path returned in a custom format defined
   * in CephFileSystem.
   * Inputs:
   *  String path: The path to stat.
   *  Stat fill: The stat object to fill.
   * Returns: true if the stat is successful, false otherwise.
   */
  abstract protected boolean ceph_stat(String path, CephFileSystem.Stat fill) throws IOException;

  /*
   * Check how many times a file should be replicated. If it is,
   * degraded it may not actually be replicated this often.
   * Inputs:
   *  int fh: a file descriptor
   * Returns: an int containing the number of times replicated.
   */
  abstract protected int ceph_replication(String path) throws IOException;

  /*
   * Find the IP address of the primary OSD for a given file and offset.
   * Inputs:
   *  int fh: The filehandle for the file.
   *  long offset: The offset to get the location of.
   * Returns: an array of String of the location as IP, or NULL if there is an error.
   */
  abstract protected String[] ceph_hosts(int fh, long offset);

  /*
   * Set the mtime and atime for a given path.
   * Inputs:
   *  String path: The path to set the times for.
   *  long mtime: The mtime to set, in millis since epoch (-1 to not set).
   *  long atime: The atime to set, in millis since epoch (-1 to not set)
   * Returns: 0 if successful, an error code otherwise.
   */
  abstract protected int ceph_setTimes(String path, long mtime, long atime);

  /*
   * Get the current position in a file (as a long) of a given filehandle.
   * Returns: (long) current file position on success, or a
   *  negative error code on failure.
   */
  abstract protected long ceph_getpos(int fh) throws IOException;

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

  /*
   * Seeks to the given position in the given file.
   * Inputs:
   *  int fh: The filehandle to seek in.
   *  long pos: The position to seek to.
   * Returns: the new position (as a long) of the filehandle on success,
   *  or a negative error code on failure.	 */
  abstract protected long ceph_seek_from_start(int fh, long pos) throws IOException;
}
