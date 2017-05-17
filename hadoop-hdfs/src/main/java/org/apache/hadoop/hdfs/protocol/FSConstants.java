/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

/************************************
 * Some handy constants
 *
 ************************************/
public interface FSConstants {
    int MIN_BLOCKS_FOR_WRITE = 5;

    // Long that indicates "leave current quota unchanged"
    long QUOTA_DONT_SET = Long.MAX_VALUE;
    long QUOTA_RESET = -1L;

    //
    // Timeouts, constants
    //
    long HEARTBEAT_INTERVAL = 3;
    long BLOCKREPORT_INTERVAL = 60 * 60 * 1000;
    long BLOCKREPORT_INITIAL_DELAY = 0;
    long LEASE_SOFTLIMIT_PERIOD = 60 * 1000;
    long LEASE_HARDLIMIT_PERIOD = 60 * LEASE_SOFTLIMIT_PERIOD;
    long LEASE_RECOVER_PERIOD = 10 * 1000; //in ms

    // We need to limit the length and depth of a path in the filesystem.  HADOOP-438
    // Currently we set the maximum length to 8k characters and the maximum depth to 1k.
    int MAX_PATH_LENGTH = 8000;
    int MAX_PATH_DEPTH = 1000;

    int BUFFER_SIZE = new Configuration().getInt("io.file.buffer.size", 4096);
    //Used for writing header etc.
    int SMALL_BUFFER_SIZE = Math.min(BUFFER_SIZE / 2, 512);
    //TODO mb@media-style.com: should be conf injected?
    long DEFAULT_BLOCK_SIZE = DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
    int DEFAULT_DATA_SOCKET_SIZE = 128 * 1024;

    int SIZE_OF_INTEGER = Integer.SIZE / Byte.SIZE;

    // SafeMode actions
    public enum SafeModeAction {
        SAFEMODE_LEAVE, SAFEMODE_ENTER, SAFEMODE_GET;
    }

    // type of the datanode report
    enum DatanodeReportType {
        ALL, LIVE, DEAD
    }

    /**
     * Distributed upgrade actions:
     *
     * 1. Get upgrade status.
     * 2. Get detailed upgrade status.
     * 3. Proceed with the upgrade if it is stuck, no matter what the status is.
     */
    enum UpgradeAction {
        GET_STATUS,
        DETAILED_STATUS,
        FORCE_PROCEED;
    }

    // Version is reflected in the dfs image and edit log files.
    // Version is reflected in the data storage file.
    // Versions are negative.
    // Decrement LAYOUT_VERSION to define a new version.
    int LAYOUT_VERSION = -32;
    // Current version:
    // -32: to handle editlog opcode conflicts with 0.20.203 during upgrades and
    // to disallow upgrade to release 0.21.
}
