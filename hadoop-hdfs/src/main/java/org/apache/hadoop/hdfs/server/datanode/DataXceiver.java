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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.MetaDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

/**
 * Thread for processing incoming/outgoing data stream.
 * 处理输出输出数据流线程
 */
class DataXceiver implements Runnable, FSConstants {
    public static final Log LOG = DataNode.LOG;
    static final Log ClientTraceLog = DataNode.ClientTraceLog;

    Socket s;
    final String remoteAddress; // address of remote side
    final String localAddress;  // local address of this daemon
    DataNode datanode;
    DataXceiverServer dataXceiverServer;

    public DataXceiver(Socket s, DataNode datanode,
                       DataXceiverServer dataXceiverServer) {

        this.s = s;
        this.datanode = datanode;
        this.dataXceiverServer = dataXceiverServer;
        dataXceiverServer.childSockets.put(s, s);
        remoteAddress = s.getRemoteSocketAddress().toString();
        localAddress = s.getLocalSocketAddress().toString();
        LOG.debug("Number of active connections is: " + datanode.getXceiverCount());
    }

    /**
     * Read/write data from/to the DataXceiveServer.
     */
    public void run() {
        DataInputStream in = null;
        try {
            in = new DataInputStream(new BufferedInputStream(NetUtils.getInputStream(s), SMALL_BUFFER_SIZE));
            short version = in.readShort();
            if (version != DataTransferProtocol.DATA_TRANSFER_VERSION) {
                throw new IOException("Version Mismatch");
            }
            boolean local = s.getInetAddress().equals(s.getLocalAddress());
            byte op = in.readByte();
            // Make sure the xciver count is not exceeded
            int curXceiverCount = datanode.getXceiverCount();
            if (curXceiverCount > dataXceiverServer.maxXceiverCount) {
                throw new IOException("xceiverCount " + curXceiverCount
                        + " exceeds the limit of concurrent xcievers "
                        + dataXceiverServer.maxXceiverCount);
            }
            long startTime = DataNode.now();
            switch (op) {
                case DataTransferProtocol.OP_READ_BLOCK:
                    readBlock(in);
                    datanode.myMetrics.addReadBlockOp(DataNode.now() - startTime);
                    if (local)
                        datanode.myMetrics.incrReadsFromLocalClient();
                    else
                        datanode.myMetrics.incrReadsFromRemoteClient();
                    break;
                case DataTransferProtocol.OP_WRITE_BLOCK:
                    writeBlock(in);
                    datanode.myMetrics.addWriteBlockOp(DataNode.now() - startTime);
                    if (local)
                        datanode.myMetrics.incrWritesFromLocalClient();
                    else
                        datanode.myMetrics.incrWritesFromRemoteClient();
                    break;
                case DataTransferProtocol.OP_REPLACE_BLOCK: // for balancing purpose; send to a destination
                    replaceBlock(in);
                    datanode.myMetrics.addReplaceBlockOp(DataNode.now() - startTime);
                    break;
                case DataTransferProtocol.OP_COPY_BLOCK:
                    // for balancing purpose; send to a proxy source
                    copyBlock(in);
                    datanode.myMetrics.addCopyBlockOp(DataNode.now() - startTime);
                    break;
                case DataTransferProtocol.OP_BLOCK_CHECKSUM: //get the checksum of a block
                    getBlockChecksum(in);
                    datanode.myMetrics.addBlockChecksumOp(DataNode.now() - startTime);
                    break;
                default:
                    throw new IOException("Unknown opcode " + op + " in data stream");
            }
        } catch (Throwable t) {
            LOG.error(datanode.dnRegistration + ":DataXceiver", t);
        } finally {
            LOG.debug(datanode.dnRegistration + ":Number of active connections is: "
                    + datanode.getXceiverCount());
            IOUtils.closeStream(in);
            IOUtils.closeSocket(s);
            dataXceiverServer.childSockets.remove(s);
        }
    }

    /**
     * Read a block from the disk.
     *
     * @param in The stream to read from
     * @throws IOException
     */
    private void readBlock(DataInputStream in) throws IOException {
        // 要读取的数据块标识，数据节点通过它定位数据块
        long blockId = in.readLong();
        // 用于版本检查，防止读取错误的数据
        long generationStamp = in.readLong();
        Block block = new Block(blockId, 0, generationStamp);
        // 要读取的数据位于数据块中的位置
        long startOffset = in.readLong();
        // 客户端需要读取的数据长度
        long length = in.readLong();
        // 发起读请求的客户端名字
        String clientName = Text.readString(in);
        Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
        //访问令牌
        accessToken.readFields(in);

        OutputStream baseStream = NetUtils.getOutputStream(s, datanode.socketWriteTimeout);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));

        if (datanode.isBlockTokenEnabled) {
            try {
                datanode.blockTokenSecretManager.checkAccess(accessToken, null, block,
                        BlockTokenSecretManager.AccessMode.READ);
            } catch (InvalidToken e) {
                try {
                    out.writeShort(DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN);
                    out.flush();
                    throw new IOException("Access token verification failed, for client "
                            + remoteAddress + " for OP_READ_BLOCK for block " + block);
                } finally {
                    IOUtils.closeStream(out);
                }
            }
        }
        // send the block
        BlockSender blockSender = null;
        final String clientTraceFmt = clientName.length() > 0 && ClientTraceLog.isInfoEnabled()
                ? String.format(DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress,
                "%d", "HDFS_READ", clientName, "%d", datanode.dnRegistration.getStorageID(), block, "%d")
                : datanode.dnRegistration + " Served block " + block + " to " + s.getInetAddress();
        try {
            try {
                blockSender = new BlockSender(block, startOffset, length,
                        true, true, false, datanode, clientTraceFmt);
            } catch (IOException e) {
                out.writeShort(DataTransferProtocol.OP_STATUS_ERROR);
                throw e;
            }

            out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS); // send op status
            long read = blockSender.sendBlock(out, baseStream, null); // send data

            if (blockSender.isBlockReadFully()) {
                // See if client verification succeeded.
                // This is an optional response from client.
                try {
                    if (in.readShort() == DataTransferProtocol.OP_STATUS_CHECKSUM_OK &&
                            datanode.blockScanner != null) {
                        datanode.blockScanner.verifiedByClient(block);
                    }
                } catch (IOException ignored) {
                }
            }

            datanode.myMetrics.incrBytesRead((int) read);
            datanode.myMetrics.incrBlocksRead();
        } catch (SocketException ignored) {
            // Its ok for remote side to close the connection anytime.
            datanode.myMetrics.incrBlocksRead();
        } catch (IOException ioe) {
      /* What exactly should we do here?
       * Earlier version shutdown() datanode if there is disk error.
       */
            LOG.warn(datanode.dnRegistration + ":Got exception while serving " +
                    block + " to " +
                    s.getInetAddress() + ":\n" +
                    StringUtils.stringifyException(ioe));
            throw ioe;
        } finally {
            IOUtils.closeStream(out);
            IOUtils.closeStream(blockSender);
        }
    }

    /**
     * Write a block to disk.
     *
     * @param in The stream to read from
     * @throws IOException
     */
    private void writeBlock(DataInputStream in) throws IOException {
        /*第一部分：从输入流中读取请求参数*/
        LOG.debug("writeBlock receive buf size " + s.getReceiveBufferSize() +
                " tcp no delay " + s.getTcpNoDelay());

        Block block = new Block(in.readLong(),//读取blockID
                dataXceiverServer.estimateBlockSize, in.readLong()); // 读取generationStamp，版本号
        LOG.info("Receiving block " + block +
                " src: " + remoteAddress +
                " dest: " + localAddress);
        int pipelineSize = in.readInt(); // 参与到写过程的所有数据节点的个数
        boolean isRecovery = in.readBoolean(); // 是否是数据恢复过程
        String client = Text.readString(in); // 客户端名字
        boolean hasSrcDataNode = in.readBoolean(); // 是否包含源信息
        DatanodeInfo srcDataNode = null;
        if (hasSrcDataNode) {
            srcDataNode = new DatanodeInfo();
            srcDataNode.readFields(in);// 读取源信息
        }
        int numTargets = in.readInt(); //得到数据目标列表大小
        if (numTargets < 0) {
            throw new IOException("Mislabelled incoming datastream.");
        }
        DatanodeInfo targets[] = new DatanodeInfo[numTargets];// 当前数据节点的下游数据推送目标列表
        for (int i = 0; i < targets.length; i++) {
            DatanodeInfo tmp = new DatanodeInfo();
            tmp.readFields(in);
            targets[i] = tmp;
        }
        //访问令牌
        Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
        accessToken.readFields(in);
        DataOutputStream replyOut = null;   // stream to prev target
        replyOut = new DataOutputStream(NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
        if (datanode.isBlockTokenEnabled) {//验证访问令牌
            try {
                datanode.blockTokenSecretManager.checkAccess(accessToken, null, block,
                        BlockTokenSecretManager.AccessMode.WRITE);
            } catch (InvalidToken e) {
                try {
                    if (client.length() != 0) {
                        replyOut.writeShort((short) DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN);
                        Text.writeString(replyOut, datanode.dnRegistration.getName());
                        replyOut.flush();
                    }
                    throw new IOException("Access token verification failed, for client "
                            + remoteAddress + " for OP_WRITE_BLOCK for block " + block);
                } finally {
                    IOUtils.closeStream(replyOut);
                }
            }
        }

        /*第二部分：构造BlockReceiver*/
        DataOutputStream mirrorOut = null;  // 当前数据节点下游输出流
        DataInputStream mirrorIn = null;    // 当前数据节点下游输入流
        Socket mirrorSocket = null;           // 下游Socket对象
        String mirrorNode = null;           // the name:port of next target
        String firstBadLink = "";           // first datanode that failed in connection setup
        short mirrorInStatus = (short) DataTransferProtocol.OP_STATUS_SUCCESS;

        BlockReceiver blockReceiver = null; // responsible for data handling
        try {
            // open a block receiver and check if the block does not exist
            blockReceiver = new BlockReceiver(block, in,
                    s.getRemoteSocketAddress().toString(),
                    s.getLocalSocketAddress().toString(),
                    isRecovery, client, srcDataNode, datanode);

            //
            // Open network conn to backup machine, if
            // appropriate
            //
            /*第三部分：如果存在下游DataNode，则建立输入输出流*/
            if (targets.length > 0) { // 如果存在下游 DataNode
                InetSocketAddress mirrorTarget;
                // Connect to backup machine
                mirrorNode = targets[0].getName();
                mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
                mirrorSocket = datanode.newSocket();
                try {
                    int timeoutValue = datanode.socketTimeout +
                            (HdfsConstants.READ_TIMEOUT_EXTENSION * numTargets);
                    int writeTimeout = datanode.socketWriteTimeout +
                            (HdfsConstants.WRITE_TIMEOUT_EXTENSION * numTargets);
                    NetUtils.connect(mirrorSocket, mirrorTarget, timeoutValue);
                    mirrorSocket.setSoTimeout(timeoutValue);
                    mirrorSocket.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
                    mirrorOut = new DataOutputStream(
                            new BufferedOutputStream(
                                    NetUtils.getOutputStream(mirrorSocket, writeTimeout),
                                    SMALL_BUFFER_SIZE));
                    mirrorIn = new DataInputStream(NetUtils.getInputStream(mirrorSocket));

                    // Write header: Copied from DFSClient.java!
                    mirrorOut.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
                    mirrorOut.write(DataTransferProtocol.OP_WRITE_BLOCK); //请求码
                    mirrorOut.writeLong(block.getBlockId()); // blockID
                    mirrorOut.writeLong(block.getGenerationStamp()); //版本号
                    mirrorOut.writeInt(pipelineSize);
                    mirrorOut.writeBoolean(isRecovery);
                    Text.writeString(mirrorOut, client); //客户端名字
                    mirrorOut.writeBoolean(hasSrcDataNode);
                    if (hasSrcDataNode) { // 传递源节点信息
                        srcDataNode.write(mirrorOut);
                    }
                    mirrorOut.writeInt(targets.length - 1);
                    for (int i = 1; i < targets.length; i++) {
                        targets[i].write(mirrorOut);
                    }
                    accessToken.write(mirrorOut);

                    blockReceiver.writeChecksumHeader(mirrorOut);// 向下游写数据校验信息
                    mirrorOut.flush(); // 数据刷出

                    // read connect ack (only for clients, not for replication req)
                    // 如果不是复制请求（即从客户端发起的写请求），从下游读取应答
                    if (client.length() != 0) {

                        /*如果下游构造BlockReceiver出错，则会关闭所有socket和stream ，此时readShort会出错抛出异常*/
                        /*被下面的catch块捕获*/
                        mirrorInStatus = mirrorIn.readShort();
                        firstBadLink = Text.readString(mirrorIn);
                        if (LOG.isDebugEnabled() || mirrorInStatus != DataTransferProtocol.OP_STATUS_SUCCESS) {
                            LOG.info("Datanode " + targets.length +
                                    " got response for connect ack " +
                                    " from downstream datanode with firstbadlink as " +
                                    firstBadLink);
                        }
                    }

                } catch (IOException e) {
                    // 如果是复制请求
                    if (client.length() != 0) {
                        //在往下一个数据节点 【写请求】 的过程中出错，向上游写应答
                        replyOut.writeShort((short) DataTransferProtocol.OP_STATUS_ERROR);
                        Text.writeString(replyOut, mirrorNode);
                        replyOut.flush();
                    }
                    IOUtils.closeStream(mirrorOut);
                    mirrorOut = null;
                    IOUtils.closeStream(mirrorIn);
                    mirrorIn = null;
                    IOUtils.closeSocket(mirrorSocket);
                    mirrorSocket = null;
                    if (client.length() > 0) {
                        throw e;
                    } else {
                        LOG.info(datanode.dnRegistration + ":Exception transfering block " +
                                block + " to mirror " + mirrorNode +
                                ". continuing without the mirror.\n" +
                                StringUtils.stringifyException(e));
                    }
                }
            }

            // 顺利完成各类初始化任务，写应答
            if (client.length() != 0) {
                if (LOG.isDebugEnabled() || mirrorInStatus != DataTransferProtocol.OP_STATUS_SUCCESS) {
                    LOG.info("Datanode " + targets.length +
                            " forwarding connect ack to upstream firstbadlink is " +
                            firstBadLink);
                }
                /*向上游回复成功的ack*/
                replyOut.writeShort(mirrorInStatus);
                Text.writeString(replyOut, firstBadLink);
                replyOut.flush();
            }

            // receive the block and mirror to the next target
            String mirrorAddr = (mirrorSocket == null) ? null : mirrorNode;

            // 写输出的逻辑交给 BlockReceiver.receiveBlock()
            blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,
                    mirrorAddr, null, targets.length);

            // 如果当前是复制请求，则向NameNode进行确认汇报
            // 如果是客户端写数据，则汇报工作交给 PacketResponder
            if (client.length() == 0) {

                datanode.notifyNamenodeReceivedBlock(block, DataNode.EMPTY_DEL_HINT);
                LOG.info("Received block " + block +
                        " src: " + remoteAddress +
                        " dest: " + localAddress +
                        " of size " + block.getNumBytes());
            }

            if (datanode.blockScanner != null) {
                // 向数据块扫描器 注册新的数据块
                datanode.blockScanner.addBlock(block);
            }

        } catch (IOException ioe) {
            LOG.info("writeBlock " + block + " received exception " + ioe);
            throw ioe;
        } finally {
            // close all opened streams
            IOUtils.closeStream(mirrorOut);
            IOUtils.closeStream(mirrorIn);
            IOUtils.closeStream(replyOut);
            IOUtils.closeSocket(mirrorSocket);
            IOUtils.closeStream(blockReceiver);
        }
    }

    /**
     * Get block checksum (MD5 of CRC32).
     *
     * @param in
     */
    void getBlockChecksum(DataInputStream in) throws IOException {
        final Block block = new Block(in.readLong(), 0, in.readLong());
        Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
        accessToken.readFields(in);
        DataOutputStream out = new DataOutputStream(NetUtils.getOutputStream(s,
                datanode.socketWriteTimeout));
        if (datanode.isBlockTokenEnabled) {
            try {
                datanode.blockTokenSecretManager.checkAccess(accessToken, null, block,
                        BlockTokenSecretManager.AccessMode.READ);
            } catch (InvalidToken e) {
                try {
                    out.writeShort(DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN);
                    out.flush();
                    throw new IOException(
                            "Access token verification failed, for client " + remoteAddress
                                    + " for OP_BLOCK_CHECKSUM for block " + block);
                } finally {
                    IOUtils.closeStream(out);
                }
            }
        }

        final MetaDataInputStream metadataIn = datanode.data.getMetaDataInputStream(block);
        final DataInputStream checksumIn = new DataInputStream(new BufferedInputStream(
                metadataIn, BUFFER_SIZE));

        try {
            //read metadata file
            final BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
            final DataChecksum checksum = header.getChecksum();
            final int bytesPerCRC = checksum.getBytesPerChecksum();
            final long crcPerBlock = (metadataIn.getLength()
                    - BlockMetadataHeader.getHeaderSize()) / checksum.getChecksumSize();

            //compute block checksum
            final MD5Hash md5 = MD5Hash.digest(checksumIn);

            if (LOG.isDebugEnabled()) {
                LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
                        + ", crcPerBlock=" + crcPerBlock + ", md5=" + md5);
            }

            //write reply
            out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS);
            out.writeInt(bytesPerCRC);
            out.writeLong(crcPerBlock);
            md5.write(out);
            out.flush();
        } finally {
            IOUtils.closeStream(out);
            IOUtils.closeStream(checksumIn);
            IOUtils.closeStream(metadataIn);
        }
    }

    /**
     * Read a block from the disk and then sends it to a destination.
     *
     * @param in The stream to read from
     * @throws IOException
     */
    private void copyBlock(DataInputStream in) throws IOException {
        // Read in the header
        long blockId = in.readLong(); // read block id
        Block block = new Block(blockId, 0, in.readLong());
        Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
        accessToken.readFields(in);
        if (datanode.isBlockTokenEnabled) {
            try {
                datanode.blockTokenSecretManager.checkAccess(accessToken, null, block,
                        BlockTokenSecretManager.AccessMode.COPY);
            } catch (InvalidToken e) {
                LOG.warn("Invalid access token in request from "
                        + remoteAddress + " for OP_COPY_BLOCK for block " + block);
                sendResponse(s,
                        (short) DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN,
                        datanode.socketWriteTimeout);
                return;
            }
        }

        if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
            LOG.info("Not able to copy block " + blockId + " to "
                    + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
            sendResponse(s, (short) DataTransferProtocol.OP_STATUS_ERROR,
                    datanode.socketWriteTimeout);
            return;
        }

        BlockSender blockSender = null;
        DataOutputStream reply = null;
        boolean isOpSuccess = true;

        try {
            // check if the block exists or not
            blockSender = new BlockSender(block, 0, -1, false, false, false,
                    datanode);

            // set up response stream
            OutputStream baseStream = NetUtils.getOutputStream(
                    s, datanode.socketWriteTimeout);
            reply = new DataOutputStream(new BufferedOutputStream(
                    baseStream, SMALL_BUFFER_SIZE));

            // send status first
            reply.writeShort((short) DataTransferProtocol.OP_STATUS_SUCCESS);
            // send block content to the target
            long read = blockSender.sendBlock(reply, baseStream,
                    dataXceiverServer.balanceThrottler);

            datanode.myMetrics.incrBytesRead((int) read);
            datanode.myMetrics.incrBlocksRead();

            LOG.info("Copied block " + block + " to " + s.getRemoteSocketAddress());
        } catch (IOException ioe) {
            isOpSuccess = false;
            throw ioe;
        } finally {
            dataXceiverServer.balanceThrottler.release();
            if (isOpSuccess) {
                try {
                    // send one last byte to indicate that the resource is cleaned.
                    reply.writeChar('d');
                } catch (IOException ignored) {
                }
            }
            IOUtils.closeStream(reply);
            IOUtils.closeStream(blockSender);
        }
    }

    /**
     * Receive a block and write it to disk, it then notifies the namenode to
     * remove the copy from the source.
     *
     * @param in The stream to read from
     * @throws IOException
     */
    private void replaceBlock(DataInputStream in) throws IOException {
        /* 第一步：读入请求相关信息 */
        long blockId = in.readLong();
        Block block = new Block(blockId, dataXceiverServer.estimateBlockSize,
                in.readLong()); // block id & generation stamp
        String sourceID = Text.readString(in); // read del hint
        DatanodeInfo proxySource = new DatanodeInfo(); // read proxy source
        proxySource.readFields(in);
        Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
        accessToken.readFields(in);
        if (datanode.isBlockTokenEnabled) {
            try {
                datanode.blockTokenSecretManager.checkAccess(accessToken, null, block,
                        BlockTokenSecretManager.AccessMode.REPLACE);
            } catch (InvalidToken e) {
                LOG.warn("Invalid access token in request from "
                        + remoteAddress + " for OP_REPLACE_BLOCK for block " + block);
                sendResponse(s, (short) DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN,
                        datanode.socketWriteTimeout);
                return;
            }
        }

        if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
            LOG.warn("Not able to receive block " + blockId + " from "
                    + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
            sendResponse(s, (short) DataTransferProtocol.OP_STATUS_ERROR,
                    datanode.socketWriteTimeout);
            return;
        }

        /*第二步：建立到数据源节点的Socket连接*/

        Socket proxySock = null;
        DataOutputStream proxyOut = null;
        short opStatus = DataTransferProtocol.OP_STATUS_SUCCESS;
        BlockReceiver blockReceiver = null;
        DataInputStream proxyReply = null;

        try {
            InetSocketAddress proxyAddr = NetUtils.createSocketAddr(proxySource.getName());
            proxySock = datanode.newSocket();
            NetUtils.connect(proxySock, proxyAddr, datanode.socketTimeout);
            proxySock.setSoTimeout(datanode.socketTimeout);

            OutputStream baseStream = NetUtils.getOutputStream(proxySock,
                    datanode.socketWriteTimeout);
            proxyOut = new DataOutputStream(
                    new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));

            /* 第三步：发送数据拷贝请求 */
            proxyOut.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION); // transfer version
            proxyOut.writeByte(DataTransferProtocol.OP_COPY_BLOCK); // op code
            proxyOut.writeLong(block.getBlockId()); // block id
            proxyOut.writeLong(block.getGenerationStamp()); // block id
            accessToken.write(proxyOut);
            proxyOut.flush();

            // receive the response from the proxy
            proxyReply = new DataInputStream(new BufferedInputStream(
                    NetUtils.getInputStream(proxySock), BUFFER_SIZE));
            short status = proxyReply.readShort();
            if (status != DataTransferProtocol.OP_STATUS_SUCCESS) {
                if (status == DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN) {
                    throw new IOException("Copy block " + block + " from "
                            + proxySock.getRemoteSocketAddress()
                            + " failed due to access token error");
                }
                throw new IOException("Copy block " + block + " from "
                        + proxySock.getRemoteSocketAddress() + " failed");
            }
            // open a block receiver and check if the block does not exist
            blockReceiver = new BlockReceiver(
                    block, proxyReply, proxySock.getRemoteSocketAddress().toString(),
                    proxySock.getLocalSocketAddress().toString(),
                    false, "", null, datanode);

            // 接受数据块
            blockReceiver.receiveBlock(null, null, null, null,
                    dataXceiverServer.balanceThrottler, -1);

            // 通知NameNode
            datanode.notifyNamenodeReceivedBlock(block, sourceID);

            LOG.info("Moved block " + block +
                    " from " + s.getRemoteSocketAddress());

        } catch (IOException ioe) {
            opStatus = DataTransferProtocol.OP_STATUS_ERROR;
            throw ioe;
        } finally {
            // receive the last byte that indicates the proxy released its thread resource
            if (opStatus == DataTransferProtocol.OP_STATUS_SUCCESS) {
                try {
                    proxyReply.readChar();
                } catch (IOException ignored) {
                }
            }

            // now release the thread resource
            dataXceiverServer.balanceThrottler.release();

            // send response back
            try {
                sendResponse(s, opStatus, datanode.socketWriteTimeout);
            } catch (IOException ioe) {
                LOG.warn("Error writing reply back to " + s.getRemoteSocketAddress());
            }
            IOUtils.closeStream(proxyOut);
            IOUtils.closeStream(blockReceiver);
            IOUtils.closeStream(proxyReply);
        }
    }

    /**
     * Utility function for sending a response.
     *
     * @param s        socket to write to
     * @param opStatus status message to write
     * @param timeout  send timeout
     **/
    private void sendResponse(Socket s, short opStatus, long timeout)
            throws IOException {
        DataOutputStream reply =
                new DataOutputStream(NetUtils.getOutputStream(s, timeout));
        try {
            reply.writeShort(opStatus);
            reply.flush();
        } finally {
            IOUtils.closeStream(reply);
        }
    }
}
