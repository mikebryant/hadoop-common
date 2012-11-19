/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ceph;

import org.apache.hadoop.fs.CommonConfigurationKeys;

/**
 * Configuration key constants used by CephFileSystem.
 */
public class CephConfigKeys extends CommonConfigurationKeys {
  public static final String CEPH_BLOCK_SIZE_KEY = "fs.ceph.block.size";
  public static final long   CEPH_BLOCK_SIZE_DEFAULT = 64*1024*1024;

  public static final String CEPH_CONF_FILE_KEY = "fs.ceph.conf.file";
  public static final String CEPH_CONF_FILE_DEFAULT = null;

  public static final String CEPH_CONF_OPTS_KEY = "fs.ceph.conf.options";
  public static final String CEPH_CONF_OPTS_DEFAULT = null;

  public static final String CEPH_REPLICATION_KEY = "fs.ceph.replication";
  public static final short  CEPH_REPLICATION_DEFAULT = 3;

  public static final String CEPH_ROOT_DIR_KEY = "fs.ceph.root.dir";
  public static final String CEPH_ROOT_DIR_DEFAULT = "/";
}
