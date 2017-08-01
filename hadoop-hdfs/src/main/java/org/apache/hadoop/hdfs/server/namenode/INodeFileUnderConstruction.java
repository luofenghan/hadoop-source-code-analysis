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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

import java.io.IOException;

/**
 * 处于构建状态的文件索引节点
 * 当客户端正在为写 数据打开HDFS文件时，该文件就处于构建状态
 */
class INodeFileUnderConstruction extends INodeFile {
    String clientName;// 写文件的客户端，也是这个文件租约的文件所有者
    private final String clientMachine;//客户端所在的主机
    private final DatanodeDescriptor clientNode;// 如果客户端运行在集群内的某个数据节点上，数据节点的信息

    private int primaryNodeIndex = -1; //租约恢复时的主数据节点，保存了恢复时的主数据节点索引
    private DatanodeDescriptor[] targets = null;//参与到当前数据块的数据节点成员列表
    private long lastRecoveryTime = 0; //租约恢复的开始时间

    INodeFileUnderConstruction(PermissionStatus permissions,
                               short replication,
                               long preferredBlockSize,
                               long modTime,
                               String clientName,
                               String clientMachine,
                               DatanodeDescriptor clientNode) {
        super(permissions.applyUMask(UMASK), 0, replication, modTime, modTime,
                preferredBlockSize);
        this.clientName = clientName;
        this.clientMachine = clientMachine;
        this.clientNode = clientNode;
    }

    INodeFileUnderConstruction(byte[] name,
                               short blockReplication,
                               long modificationTime,
                               long preferredBlockSize,
                               BlockInfo[] blocks,
                               PermissionStatus perm,
                               String clientName,
                               String clientMachine,
                               DatanodeDescriptor clientNode) {
        super(perm, blocks, blockReplication, modificationTime, modificationTime,
                preferredBlockSize);
        setLocalName(name);
        this.clientName = clientName;
        this.clientMachine = clientMachine;
        this.clientNode = clientNode;
    }

    String getClientName() {
        return clientName;
    }

    void setClientName(String newName) {
        clientName = newName;
    }

    String getClientMachine() {
        return clientMachine;
    }

    DatanodeDescriptor getClientNode() {
        return clientNode;
    }

    /**
     * Is this inode being constructed?
     */
    @Override
    boolean isUnderConstruction() {
        return true;
    }

    DatanodeDescriptor[] getTargets() {
        return targets;
    }

    void setTargets(DatanodeDescriptor[] targets) {
        this.targets = targets;
        this.primaryNodeIndex = -1;
    }

    /**
     * add this target if it does not already exists
     */
    void addTarget(DatanodeDescriptor node) {
        if (this.targets == null) {
            this.targets = new DatanodeDescriptor[0];
        }

        for (DatanodeDescriptor target : this.targets) {
            if (target.equals(node)) {
                return;  // target already exists
            }
        }

        // allocate new data structure to store additional target
        DatanodeDescriptor[] newt = new DatanodeDescriptor[targets.length + 1];
        System.arraycopy(this.targets, 0, newt, 0, targets.length);
        newt[targets.length] = node;
        this.targets = newt;
        this.primaryNodeIndex = -1;
    }

    //
    // converts a INodeFileUnderConstruction into a INodeFile
    // use the modification time as the access time
    //
    INodeFile convertToInodeFile() {
        return new INodeFile(getPermissionStatus(),
                getBlocks(),
                getReplication(),
                getModificationTime(),
                getModificationTime(),
                getPreferredBlockSize());

    }

    /**
     * remove a block from the block list. This block should be
     * the last one on the list.
     */
    void removeBlock(Block oldblock) throws IOException {
        if (blocks == null) {
            throw new IOException("Trying to delete non-existant block " + oldblock);
        }
        int lastBlock = blocks.length - 1;
        if (!blocks[lastBlock].equals(oldblock)) {
            throw new IOException("Trying to delete non-last block " + oldblock);
        }

        //copy to a new list
        BlockInfo[] newlist = new BlockInfo[lastBlock];

        /*将最后一个块删除了*/
        System.arraycopy(blocks, 0, newlist, 0, lastBlock);
        blocks = newlist;

        // Remove the block locations for the last block.
        targets = null;
    }

    synchronized void setLastBlock(BlockInfo newblock, DatanodeDescriptor[] newtargets) throws IOException {
        if (blocks == null || blocks.length == 0) {
            throw new IOException("Trying to update non-existant block (newblock="
                    + newblock + ")");
        }
        BlockInfo oldLast = blocks[blocks.length - 1];
        if (oldLast.getBlockId() != newblock.getBlockId()) {
            // This should not happen - this means that we're performing recovery
            // on an internal block in the file!
            NameNode.stateChangeLog.error(
                    "Trying to commit block synchronization for an internal block on"
                            + " inode=" + this
                            + " newblock=" + newblock + " oldLast=" + oldLast);
            throw new IOException("Trying to update an internal block of " +
                    "pending file " + this);
        }

        if (oldLast.getGenerationStamp() > newblock.getGenerationStamp()) {
            NameNode.stateChangeLog.warn(
                    "Updating last block " + oldLast + " of inode " +
                            "under construction " + this + " with a block that " +
                            "has an older generation stamp: " + newblock);
        }

        blocks[blocks.length - 1] = newblock;
        setTargets(newtargets);
        lastRecoveryTime = 0;
    }

    /**
     * Initialize lease recovery for this object
     * 在正常工作的数据流管道成员中选择一个数据节点，作为恢复的主节点，其他节点作为参与的主节点
     * 并讲这些信息添加到主数据节点的描述符中，等待该数据节点的心跳上报
     */
    void assignPrimaryDatanode() {
        //assign the first alive datanode as the primary datanode

        if (targets.length == 0) {
            NameNode.stateChangeLog.warn("BLOCK*"
                    + " INodeFileUnderConstruction.initLeaseRecovery:"
                    + " No blocks found, lease removed.");
        }

        int previous = primaryNodeIndex;
        //find an alive datanode beginning from previous
        for (int i = 1; i <= targets.length; i++) {
            int j = (previous + i) % targets.length;
            if (targets[j].isAlive) {
                DatanodeDescriptor primary = targets[primaryNodeIndex = j];
                primary.addBlockToBeRecovered(blocks[blocks.length - 1], targets);
                NameNode.stateChangeLog.info("BLOCK* " + blocks[blocks.length - 1]
                        + " recovery started, primary=" + primary);
                return;
            }
        }
    }

    /**
     * Update lastRecoveryTime if expired.
     *
     * @return true if lastRecoveryTimeis updated.
     */
    synchronized boolean setLastRecoveryTime(long now) {
        boolean expired = now - lastRecoveryTime > NameNode.LEASE_RECOVER_PERIOD;
        if (expired) {
            lastRecoveryTime = now;
        }
        return expired;
    }
}
