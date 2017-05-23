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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/**********************************************************************
 * Protocol that a DFS datanode uses to communicate with the NameNode.
 * It's used to upload current load information and block reports.
 *
 * The only way a NameNode can communicate with a DataNode is by
 * returning values from these functions.
 *
 **********************************************************************/
@KerberosInfo(
        serverPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
        clientPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY)
public interface DatanodeProtocol extends VersionedProtocol {
    /**
     * 25: Serialized format of BlockTokenIdentifier changed to contain
     * multiple blocks within a single BlockTokenIdentifier
     * <p>
     * (bumped to 25 to bring in line with trunk)
     */
    long versionID = 25L;

    // error code
    int NOTIFY = 0;
    int DISK_ERROR = 1; // there are still valid volumes on DN
    int INVALID_BLOCK = 2;
    int FATAL_DISK_ERROR = 3; // no valid volumes left on DN

    /**
     * Determines actions that data node should perform
     * when receiving a datanode command.
     */
    int DNA_UNKNOWN = 0;    // unknown action
    int DNA_TRANSFER = 1;   // 数据块复制
    int DNA_INVALIDATE = 2; // 数据块删除
    int DNA_SHUTDOWN = 3;   // 关闭数据节点
    int DNA_REGISTER = 4;   // 数据节点重新注册
    int DNA_FINALIZE = 5;   // 提交上一次升级
    int DNA_RECOVERBLOCK = 6;  // 数据块恢复
    int DNA_ACCESSKEYUPDATE = 7;  // 升级访问秘钥
    int DNA_BALANCERBANDWIDTHUPDATE = 8; // 更新平衡器可用带宽

    /**
     * Register Datanode.
     *
     * @return updated {@link org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration}, which contains
     * new storageID if the datanode did not have one and
     * registration ID for further communication.
     * @see org.apache.hadoop.hdfs.server.datanode.DataNode#dnRegistration
     * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem#registerDatanode(DatanodeRegistration)
     */
    DatanodeRegistration register(DatanodeRegistration registration) throws IOException;

    /**
     * sendHeartbeat() tells the NameNode that the DataNode is still
     * alive and well.  Includes some status info, too.
     * It also gives the NameNode a chance to return
     * an array of "DatanodeCommand" objects.
     * A DatanodeCommand tells the DataNode to invalidate local block(s),
     * or to copy them to other DataNodes, etc.
     */
    DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration,
                                    long capacity,
                                    long dfsUsed, long remaining,
                                    int xmitsInProgress,
                                    int xceiverCount) throws IOException;

    /**
     * blockReport() tells the NameNode about all the locally-stored blocks.
     * The NameNode returns an array of Blocks that have become obsolete
     * and should be deleted.  This function is meant to upload *all*
     * the locally-stored blocks.  It's invoked upon startup and then
     * infrequently afterwards.
     * 当注册成功后，数据节点上报所管理的全部数据块信息
     * 帮助名字节点构建数据块和数据节点的映射关系
     *
     * @param registration 用于标识数据节点的身份，因为IPC不支持会话，需要数据节点提供身份
     * @param blocks       - the block list as an array of longs.
     *                     Each block is represented as 2 longs.
     *                     This is done instead of Block[] to reduce memory used by block reports.
     * @return - the next command for DN to process. NameNode的指令，通知数据节点执行一些操作，如重新注册、发送心跳或者删除本地磁盘上的数据块。
     * @throws IOException
     */
    DatanodeCommand blockReport(DatanodeRegistration registration, long[] blocks) throws IOException;

    /**
     * blocksBeingWrittenReport() tells the NameNode about the blocks-being-
     * written information
     * 向NameNode报告当前正在处于写状态的数据块信息，帮助NameNode进行租约恢复
     *
     * @param registration
     * @param blocks
     * @throws IOException
     */
    void blocksBeingWrittenReport(DatanodeRegistration registration,
                                  long[] blocks) throws IOException;

    /**
     * blockReceived() allows the DataNode to tell the NameNode about
     * recently-received block data, with a hint for pereferred replica
     * to be deleted when there is any excessive blocks.
     * For example, whenever client code
     * writes a new Block here, or another DataNode copies a Block to
     * this DataNode, it will call blockReceived().
     *
     * 数据节点向NameNode报告它已经完整的接受一个数据块
     *
     * 数据块接受的来源可以是客户端或者其他节点。
     */
    void blockReceived(DatanodeRegistration registration,//提交数据块的数据节点DatanodeRegistration对象
                       Block blocks[], // 包含了接受数据块的信息
                       String[] delHints) throws IOException;

    /**
     * errorReport() tells the NameNode about something that has gone
     * awry.  Useful for debugging.
     */
    void errorReport(DatanodeRegistration registration,
                     int errorCode,
                     String msg) throws IOException;

    /**
     *  当数据节点启动之后，会和NameNode进行握手
     * @return
     * @throws IOException
     */
    NamespaceInfo versionRequest() throws IOException;

    /**
     * This is a very general way to send a command to the name-node during
     * distributed upgrade process.
     * <p>
     * The generosity is because the variety of upgrade commands is unpredictable.
     * The reply from the name-node is also received in the form of an upgrade
     * command.
     *
     * @return a reply in the form of an upgrade command
     */
    UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException;

    /**
     * same as {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#reportBadBlocks(LocatedBlock[])}
     * }
     */
    void reportBadBlocks(LocatedBlock[] blocks) throws IOException;

    /**
     * Get the next GenerationStamp to be associated with the specified
     * block.
     * 用于向NameNode 申请一个新的数据块版本号
     *
     * @param block  block
     * @param fromNN if it is for lease recovery initiated by NameNode
     * @return a new generation stamp
     */
    long nextGenerationStamp(Block block, boolean fromNN) throws IOException;

    /**
     * Commit block synchronization in lease recovery
     * 数据块恢复完成后，无论成功与否，都要报告数据块的恢复情况
     * @param block 进行恢复的数据块
     * @param newgenerationstamp 通过nextGenerationStamp申请的新版本号
     * @param newlength 数据块恢复后的新长度
     * @param closeFile 所述文件是否由NameNOde关闭
     * @param deleteblock 是否删除名字节点上的数据块信息和成功参与数据块恢复的数据节点列表
     *
     */
    void commitBlockSynchronization(Block block,
                                    long newgenerationstamp, long newlength,
                                    boolean closeFile, boolean deleteblock, DatanodeID[] newtargets) throws IOException;
}
