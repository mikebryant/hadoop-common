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
  abstract short getDefaultReplication();
  abstract short get_file_replication(Path path) throws IOException;
  abstract int write(int fd, byte[] buf, long size, long offset) throws IOException;
  abstract int read(int fd, byte[] buf, long size, long offset) throws IOException;
  abstract void mkdirs(Path path, int mode) throws IOException;
}
