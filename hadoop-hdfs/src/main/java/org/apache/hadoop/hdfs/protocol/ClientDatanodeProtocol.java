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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSelector;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

import java.io.IOException;

/**
 * An client-datanode protocol for block recovery
 */
@KerberosInfo(
        serverPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY)
@TokenInfo(BlockTokenSelector.class)
public interface ClientDatanodeProtocol extends VersionedProtocol {
    Log LOG = LogFactory.getLog(ClientDatanodeProtocol.class);

    /**
     * 4: never return null and always return a newly generated access token
     */
    long versionID = 4L;

    /**
     * 启动指定块的基于时间戳的版本恢复
     *
     * @param block      携带了被恢复数据块的信息
     * @param keepLength 恢复策略的选择
     *                   如果为true，则只恢复长度和传入数据块长度block 相同的数据块
     *                   如果为false，由主数据节点获取参与到恢复过程中的各个数据节点上的数据块长度，计算最小值，并讲这些数据节点上的数据块截断到该值
     * @param targets    参与到恢复过程的数据节点列表
     * @return either a new generation stamp, or the original generation stamp.
     * Regardless of whether a new generation stamp is returned, a newly
     * generated access token is returned as part of the return value.
     * @throws IOException
     */
    LocatedBlock recoverBlock(Block block, boolean keepLength, DatanodeInfo[] targets) throws IOException;

    /**
     * Returns a block object that contains the specified block object
     * from the specified Datanode.
     * 从指定的数据节点上获取执行的数据块信息
     *
     * @param block the specified block
     * @return the Block object from the specified Datanode
     * @throws IOException if the block does not exist
     */
    Block getBlockInfo(Block block) throws IOException;

    /**
     * Retrieves the path names of the block file and metadata file stored on the
     * local file system.
     * <p>
     * In order for this method to work, one of the following should be satisfied:
     * <ul>
     * <li>
     * The client user must be configured at the datanode to be able to use this
     * method.</li>
     * <li>
     * When security is enabled, kerberos authentication must be used to connect
     * to the datanode.</li>
     * </ul>
     *
     * @param block the specified block on the local datanode
     * @param token the block access token.
     * @return the BlockLocalPathInfo of a block
     * @throws IOException on error
     */
    BlockLocalPathInfo getBlockLocalPathInfo(Block block,
                                             Token<BlockTokenIdentifier> token) throws IOException;
}
