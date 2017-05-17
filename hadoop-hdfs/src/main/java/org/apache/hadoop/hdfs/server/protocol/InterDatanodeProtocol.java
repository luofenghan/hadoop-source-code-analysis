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

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/**
 * An inter-datanode protocol for updating generation stamp
 */
@KerberosInfo(
        serverPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY,
        clientPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY)
public interface InterDatanodeProtocol extends VersionedProtocol {
    Log LOG = LogFactory.getLog(InterDatanodeProtocol.class);

    /**
     * 3: added a finalize parameter to updateBlock
     */
    long versionID = 3L;

    /**
     * @return the BlockMetaDataInfo of a block;
     * null if the block is not found
     * 该方法没有被其他代码调用，用于自定义实现HDFS的维护工具
     */
    BlockMetaDataInfo getBlockMetaDataInfo(Block block) throws IOException;

    /**
     * Begin recovery on a block - this interrupts writers and returns the
     * necessary metadata for recovery to begin.
     * 用于获得参与到数据块恢复过程的各个数据节点上的数据块信息
     *
     * @return null if the block is not found
     */
    BlockRecoveryInfo startBlockRecovery(Block block) throws IOException;

    /**
     * Update the block to the new generation stamp and length.
     * 携带了数据块恢复前后的信息，用于通知执行操作的datanode 在完成更新后，是否提交数据块
     */
    void updateBlock(Block oldblock, Block newblock, boolean finalize) throws IOException;
}
