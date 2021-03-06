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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * This data structure is a data structure that is internal
 * to the namenode. It is *not* sent over-the-wire to the Client
 * or the Datnodes. Neither is it stored persistently in the
 * fsImage.

 **************************************************/
public class DatanodeDescriptor extends DatanodeInfo {

    // Stores status of decommissioning.
    // If node is not decommissioning, do not use this object for anything.
    DecommissioningStatus decommissioningStatus = new DecommissioningStatus();

    /**
     * Block and targets pair
     * 数据块和目标数据节点
     * 用于数据块的恢复和复制
     */
    public static class BlockTargetPair {

        public final Block block;  /*数据块*/
        public final DatanodeDescriptor[] targets;  /*目标数据节点*/

        BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
            this.block = block;
            this.targets = targets;
        }

    }

    /**
     * A BlockTargetPair queue.
     */
    private static class BlockQueue {
        private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();

        /**
         * Size of the queue
         */
        synchronized int size() {
            return blockq.size();
        }

        /**
         * Enqueue
         */
        synchronized boolean offer(Block block, DatanodeDescriptor[] targets) {
            return blockq.offer(new BlockTargetPair(block, targets));
        }

        /**
         * Dequeue
         */
        synchronized List<BlockTargetPair> poll(int numBlocks) {
            if (numBlocks <= 0 || blockq.isEmpty()) {
                return null;
            }

            List<BlockTargetPair> results = new ArrayList<BlockTargetPair>();
            for (; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
                results.add(blockq.poll());
            }
            return results;
        }
    }

    /*数据节点保存的数据块队列的头结点*/
    private volatile BlockInfo blockList = null;

    // isAlive == heartbeats.contains(this)
    // This is an optimization, because contains takes O(n) time on Arraylist
    protected boolean isAlive = false;/*节点是否失效*/

    protected boolean needKeyUpdate = false;

    // A system administrator can tune the balancer bandwidth parameter
    // (dfs.balance.bandwidthPerSec) dynamically by calling
    // "dfsadmin -setBalanacerBandwidth <newbandwidth>", at which point the
    // following 'bandwidth' variable gets updated with the new value for each
    // node. Once the heartbeat command is issued to update the value on the
    // specified datanode, this value will be set back to 0.

    /*数据节点的${dfs.balance.bandwidthPerSec}参数，可以通过以下命令设置
    * "dfsadmin -setBalanacerBandwidth <newbandwidth>"
    * 并用DataNodeCommand更新到数据节点，更新命令发出后，bandwith会被设置为0
    * */
    private long bandwidth;

    /**
     * A queue of blocks to be replicated by this datanode
     * 该数据节点上等待 复制到其他数据节点上 的数据块
     */
    private BlockQueue replicateBlocks = new BlockQueue();
    /**
     * A queue of blocks to be recovered by this datanode
     * 该数据节点上等待 租约恢复到其他数据节点上 的数据块
     */
    private BlockQueue recoverBlocks = new BlockQueue();
    /**
     * A set of blocks to be invalidated by this datanode
     * 该数据节点上等待被删除的数据块
     */
    private Set<Block> invalidateBlocks = new TreeSet<Block>();

    /* Variables for maintaning number of blocks scheduled to be written to
     * this datanode. This count is approximate and might be slightly higger
     * in case of errors (e.g. datanode does not report if an error occurs
     * while writing the block).
     * 用于估计数据节点的负载，为写文件操作分配数据块或进行数据块复制时，
     * 优先选择比较空闲的节点作为目标
     *
     *
     */
    private int currApproxBlocksScheduled = 0;
    private int prevApproxBlocksScheduled = 0;
    private long lastBlocksScheduledRollTime = 0;
    private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600 * 1000; //10min

    // Set to false after processing first block report
    /*
    * 是否发送了第一个数据块上报
    * */
    private boolean firstBlockReport = true;

    /**
     * Default constructor
     */
    public DatanodeDescriptor() {
    }

    /**
     * DatanodeDescriptor constructor
     *
     * @param nodeID id of the data node
     */
    public DatanodeDescriptor(DatanodeID nodeID) {
        this(nodeID, 0L, 0L, 0L, 0);
    }

    /**
     * DatanodeDescriptor constructor
     *
     * @param nodeID          id of the data node
     * @param networkLocation location of the data node in network
     */
    public DatanodeDescriptor(DatanodeID nodeID,
                              String networkLocation) {
        this(nodeID, networkLocation, null);
    }

    /**
     * DatanodeDescriptor constructor
     *
     * @param nodeID          id of the data node
     * @param networkLocation location of the data node in network
     * @param hostName        it could be different from host specified for DatanodeID
     */
    public DatanodeDescriptor(DatanodeID nodeID, String networkLocation, String hostName) {
        this(nodeID, networkLocation, hostName, 0L, 0L, 0L, 0);
    }

    /**
     * DatanodeDescriptor constructor
     *
     * @param nodeID       id of the data node
     * @param capacity     capacity of the data node
     * @param dfsUsed      space used by the data node
     * @param remaining    remaing capacity of the data node
     * @param xceiverCount # of data transfers at the data node
     */
    public DatanodeDescriptor(DatanodeID nodeID,
                              long capacity,
                              long dfsUsed,
                              long remaining,
                              int xceiverCount) {
        super(nodeID);
        updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
    }

    /**
     * DatanodeDescriptor constructor
     *
     * @param nodeID          id of the data node
     * @param networkLocation location of the data node in network
     * @param capacity        capacity of the data node, including space used by non-dfs
     * @param dfsUsed         the used space by dfs datanode
     * @param remaining       remaing capacity of the data node
     * @param xceiverCount    # of data transfers at the data node
     */
    public DatanodeDescriptor(DatanodeID nodeID,
                              String networkLocation,
                              String hostName,
                              long capacity,
                              long dfsUsed,
                              long remaining,
                              int xceiverCount) {
        super(nodeID, networkLocation, hostName);
        updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
    }

    /**
     * Add data-node to the block.
     * Add block to the head of the list of blocks belonging to the data-node.
     * <p>
     * 当数据节点成功接收到一个数据块后，会通过RPC方法blockReceived()报告NameNode节点。
     * NameNode自然需要把它添加/更新到对应的数据节点的DatanodeDescriptor对象中。
     * 使用了该方法。
     */
    boolean addBlock(BlockInfo b) {
        if (!b.addNode(this))
            return false;
        // add to the head of the data-node list
        blockList = b.listInsert(blockList, this);
        return true;
    }

    /**
     * Remove block from the list of blocks belonging to the data-node.
     * Remove data-node from the block.
     */
    boolean removeBlock(BlockInfo b) {
        blockList = b.listRemove(blockList, this);
        return b.removeNode(this);
    }

    /**
     * Move block to the head of the list of blocks belonging to the data-node.
     */
    void moveBlockToHead(BlockInfo b) {
        blockList = b.listRemove(blockList, this);
        blockList = b.listInsert(blockList, this);
    }

    void resetBlocks() {
        this.capacity = 0;
        this.remaining = 0;
        this.dfsUsed = 0;
        this.xceiverCount = 0;
        this.blockList = null;
        this.invalidateBlocks.clear();
    }

    public int numBlocks() {
        return blockList == null ? 0 : blockList.listCount(this);
    }

    /**
     */
    void updateHeartbeat(long capacity, long dfsUsed, long remaining, int xceiverCount) {
        this.capacity = capacity;
        this.dfsUsed = dfsUsed;
        this.remaining = remaining;
        this.lastUpdate = System.currentTimeMillis();
        this.xceiverCount = xceiverCount;
        rollBlocksScheduled(lastUpdate);
    }

    /**
     * Iterates over the list of blocks belonging to the data-node.
     */
    static private class BlockIterator implements Iterator<Block> {
        private BlockInfo current;
        private DatanodeDescriptor node;

        BlockIterator(BlockInfo head, DatanodeDescriptor dn) {
            this.current = head;
            this.node = dn;
        }

        public boolean hasNext() {
            return current != null;
        }

        public BlockInfo next() {
            BlockInfo res = current;
            current = current.getNext(current.findDatanode(node));
            return res;
        }

        public void remove() {
            throw new UnsupportedOperationException("Sorry. can't remove.");
        }
    }

    Iterator<Block> getBlockIterator() {
        return new BlockIterator(this.blockList, this);
    }

    /**
     * Store block replication work.
     */
    void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
        assert (block != null && targets != null && targets.length > 0);
        replicateBlocks.offer(block, targets);
    }

    /**
     * Store block recovery work.
     */
    void addBlockToBeRecovered(Block block, DatanodeDescriptor[] targets) {
        assert (block != null && targets != null && targets.length > 0);
        recoverBlocks.offer(block, targets);
    }

    /**
     * Store block invalidation work.
     */
    void addBlocksToBeInvalidated(List<Block> blocklist) {
        assert (blocklist != null && blocklist.size() > 0);
        synchronized (invalidateBlocks) {
            for (Block blk : blocklist) {
                invalidateBlocks.add(blk);
            }
        }
    }

    /**
     * The number of work items that are pending to be replicated
     */
    int getNumberOfBlocksToBeReplicated() {
        return replicateBlocks.size();
    }

    /**
     * The number of block invalidation items that are pending to
     * be sent to the datanode
     */
    int getNumberOfBlocksToBeInvalidated() {
        synchronized (invalidateBlocks) {
            return invalidateBlocks.size();
        }
    }

    BlockCommand getReplicationCommand(int maxTransfers) {
        List<BlockTargetPair> blocktargetlist = replicateBlocks.poll(maxTransfers);
        return blocktargetlist == null ? null :
                new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blocktargetlist);
    }

    BlockCommand getLeaseRecoveryCommand(int maxTransfers) {
        List<BlockTargetPair> blocktargetlist = recoverBlocks.poll(maxTransfers);
        return blocktargetlist == null ? null :
                new BlockCommand(DatanodeProtocol.DNA_RECOVERBLOCK, blocktargetlist);
    }

    /**
     * Remove the specified number of blocks to be invalidated
     */
    BlockCommand getInvalidateBlocks(int maxblocks) {
        Block[] deleteList = getBlockArray(invalidateBlocks, maxblocks);
        return deleteList == null ? null : new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, deleteList);
    }

    static private Block[] getBlockArray(Collection<Block> blocks, int max) {
        Block[] blockarray = null;
        synchronized (blocks) {
            int available = blocks.size();
            int n = available;
            if (max > 0 && n > 0) {
                if (max < n) {
                    n = max;
                }
                // allocate the properly sized block array ...
                blockarray = new Block[n];

                // iterate tree collecting n blocks...
                Iterator<Block> e = blocks.iterator();
                int blockCount = 0;

                while (blockCount < n && e.hasNext()) {
                    // insert into array ...
                    blockarray[blockCount++] = e.next();

                    // remove from tree via iterator, if we are removing
                    // less than total available blocks
                    if (n < available) {
                        e.remove();
                    }
                }
                assert (blockarray.length == n);

                // now if the number of blocks removed equals available blocks,
                // them remove all blocks in one fell swoop via clear
                if (n == available) {
                    blocks.clear();
                }
            }
        }
        return blockarray;
    }

    void reportDiff(BlocksMap blocksMap,
                    BlockListAsLongs newReport,/*新上报的数据块列表*/
                    Collection<Block> toAdd,/*应该添加的副本*/
                    Collection<Block> toRemove,/*移除*/
                    Collection<Block> toInvalidate/*删除*/) {
        // 添加一个特殊的数据块 delimiter 到 DatanodeDescriptor中
        // 用于分割 原有数据块 和 新添加的数据块
        BlockInfo delimiter = new BlockInfo(new Block(), 1);
        boolean added = this.addBlock(delimiter);/*添加成功后，delimiter在头结点位置*/
        assert added : "Delimiting block cannot be present in the node";

        if (newReport == null)
            newReport = new BlockListAsLongs(new long[0]);
        // scan the report and collect newly reported blocks
        // Note we are taking special precaution to limit tmp blocks allocated
        // as part this block report - which why block list is stored as longs

        /*扫描上报的数据块列表*/
        Block iblk = new Block(); // 工作Block对象，将newReport中的信息转换为对象
        Block oblk = new Block(); // for fixing genstamps
        for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
            iblk.set(newReport.getBlockId(i), newReport.getBlockLen(i), newReport.getBlockGenStamp(i));
            /*情况一：找出需要删除的数据块副本
            * 1. blocksMap中已经不存在的记录
            * 2. 所属的文件已经删除（找不到所属的INode）
            * 3. 时间戳不匹配
            * */
            BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
            if (storedBlock == null) {
                oblk.set(newReport.getBlockId(i), newReport.getBlockLen(i), GenerationStamp.WILDCARD_STAMP);/*不带时间戳查找*/
                storedBlock = blocksMap.getStoredBlock(oblk);
                if (storedBlock != null && storedBlock.getINode() != null &&
                        (storedBlock.getGenerationStamp() <= iblk.getGenerationStamp() ||
                                storedBlock.getINode().isUnderConstruction())) {
                    // accept block. It wil be cleaned up on cluster restart.
                } else {
                    storedBlock = null;
                }
            }
            if (storedBlock == null) {/*无法在文件系统目录树中找到当前数据块的信息，说明已经被删除*/
                toInvalidate.add(new Block(iblk));
                continue;
            }


            /*情况二：新添加的副本，出现在*newReport*中，但名字节点中的*DatanodeDescriptor*对象并不管理该数据块副本；*/
            if (storedBlock.findDatanode(this) < 0) {// Known block, but not on the DN
                /*如果数据节点汇报的数据块大小和 名字节点存储的大小不一致，那么返回一个新块*/
                /*addStoredBlock 接下来会挑选一个正确的大小 并 更新 BlocksMap 中的块对象*/
                if (storedBlock.getNumBytes() != iblk.getNumBytes()) {
                    toAdd.add(new Block(iblk));
                } else {
                    toAdd.add(storedBlock);
                }
                continue;
            }
            // move block to the head of the list
            /*情况三：原有的数据块*/
            this.moveBlockToHead(storedBlock);
        }
        /*情况四：需要移除的副本就是本次报告中不包含（数据节点没有），而存在于名字节点记录的副本。*/
        Iterator<Block> it = new BlockIterator(delimiter.getNext(0), this);
        while (it.hasNext()) {
            BlockInfo storedBlock = (BlockInfo) it.next();
            INodeFile file = storedBlock.getINode();
            if (file == null || !file.isUnderConstruction()) {
                toRemove.add(storedBlock);
            }
        }
        this.removeBlock(delimiter);
    }

    /**
     * Serialization for FSEditLog
     */
    void readFieldsFromFSEditLog(DataInput in) throws IOException {
        this.name = UTF8.readString(in);
        this.storageID = UTF8.readString(in);
        this.infoPort = in.readShort() & 0x0000ffff;

        this.capacity = in.readLong();
        this.dfsUsed = in.readLong();
        this.remaining = in.readLong();
        this.lastUpdate = in.readLong();
        this.xceiverCount = in.readInt();
        this.location = Text.readString(in);
        this.hostName = Text.readString(in);
        setAdminState(WritableUtils.readEnum(in, AdminStates.class));
    }

    /**
     * @return Approximate number of blocks currently scheduled to be written
     * to this datanode.
     */
    public int getBlocksScheduled() {
        return currApproxBlocksScheduled + prevApproxBlocksScheduled;
    }

    /**
     * Increments counter for number of blocks scheduled.
     */
    void incBlocksScheduled() {
        currApproxBlocksScheduled++;
    }

    /**
     * Decrements counter for number of blocks scheduled.
     */
    void decBlocksScheduled() {
        if (prevApproxBlocksScheduled > 0) {
            prevApproxBlocksScheduled--;
        } else if (currApproxBlocksScheduled > 0) {
            currApproxBlocksScheduled--;
        }
        // its ok if both counters are zero.
    }

    /**
     * Adjusts curr and prev number of blocks scheduled every few minutes.
     */
    private void rollBlocksScheduled(long now) {
        if ((now - lastBlocksScheduledRollTime) > BLOCKS_SCHEDULED_ROLL_INTERVAL) {
            prevApproxBlocksScheduled = currApproxBlocksScheduled;
            currApproxBlocksScheduled = 0;
            lastBlocksScheduledRollTime = now;
        }
    }

    class DecommissioningStatus {
        int underReplicatedBlocks;
        /*当节点处于撤销状态时使用*/
        int decommissionOnlyReplicas;
        int underReplicatedInOpenFiles;
        long startTime;

        synchronized void set(int underRep, int onlyRep, int underConstruction) {
            if (!isDecommissionInProgress()) {
                return;
            }
            underReplicatedBlocks = underRep;
            decommissionOnlyReplicas = onlyRep;
            underReplicatedInOpenFiles = underConstruction;
        }

        synchronized int getUnderReplicatedBlocks() {
            if (!isDecommissionInProgress()) {
                return 0;
            }
            return underReplicatedBlocks;
        }

        synchronized int getDecommissionOnlyReplicas() {
            if (!isDecommissionInProgress()) {
                return 0;
            }
            return decommissionOnlyReplicas;
        }

        synchronized int getUnderReplicatedInOpenFiles() {
            if (!isDecommissionInProgress()) {
                return 0;
            }
            return underReplicatedInOpenFiles;
        }

        synchronized void setStartTime(long time) {
            startTime = time;
        }

        synchronized long getStartTime() {
            if (!isDecommissionInProgress()) {
                return 0;
            }
            return startTime;
        }
    } // End of class DecommissioningStatus

    /**
     * @return Blanacer bandwidth in bytes per second for this datanode.
     */
    public long getBalancerBandwidth() {
        return this.bandwidth;
    }

    /**
     * @param bandwidth Blanacer bandwidth in bytes per second for this datanode.
     */
    public void setBalancerBandwidth(long bandwidth) {
        this.bandwidth = bandwidth;
    }

    boolean firstBlockReport() {
        return firstBlockReport;
    }

    void processedBlockReport() {
        firstBlockReport = false;
    }
}
