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

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/*****************************************************************************
 * Protocol that a secondary NameNode uses to communicate with the NameNode.
 * It's used to get part of the name node state
 * NameNode实现了该接口，该接口的调用者有两个
 * 1. Secondary NameNode
 * 2. HDFS工具：均衡器Balancer
 *****************************************************************************/
@KerberosInfo(
        serverPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
        clientPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)
public interface NamenodeProtocol extends VersionedProtocol {
    /**
     * 3: new method added: getAccessKeys()
     */
    long versionID = 3L;

    /** Get a list of blocks belonged to <code>datanode</code>
     * whose total size is equal to <code>size</code>
     * 用于获取一个数据节点上的一系列数据块及位置
     * 根据返回值，均衡器可以把数据块从该数据节点移动到其他数据节点。达到平衡各个数据节点数据块数量的目的
     * @param datanode  a data node
     * @param size      requested size
     * @return a list of blocks & their locations
     * @throws RemoteException if size is less than or equal to 0 or
    datanode does not exist
     */
    public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
            throws IOException;

    /**
     * Get the current block keys
     *
     * 用于支持移动数据块过程中需要的安全特性
     * @return ExportedBlockKeys containing current block keys
     * @throws IOException
     */
    public ExportedBlockKeys getBlockKeys() throws IOException;

    /**
     * Get the size of the current edit log (in bytes).
     * @return The number of bytes in the current edit log.
     * @throws IOException
     */
    public long getEditLogSize() throws IOException;

    /**
     * Closes the current edit log and opens a new one. The
     * call fails if the file system is in SafeMode.
     * 通知NameNode开始一次合并过程，这时NamenOde会停止使用当前编辑日志，并启用新的日志文件
     * @throws IOException
     * @return a unique token to identify this transaction. 标识一次合并点，也称检查点
     */
    CheckpointSignature rollEditLog() throws IOException;

    /**
     * Rolls the fsImage log. It removes the old fsImage, copies the
     * new image to fsImage, removes the old edits and renames edits.new
     * to edits. The call fails if any of the four files are missing.
     * @throws IOException
     */
    public void rollFsImage() throws IOException;
}
