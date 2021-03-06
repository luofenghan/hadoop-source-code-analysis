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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeInstrumentation;
import org.apache.hadoop.io.*;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * FSEditLog maintains a log of the namespace modifications.
 */
public class FSEditLog {
    private static final byte OP_INVALID = -1;
    private static final byte OP_ADD = 0;
    private static final byte OP_RENAME = 1;  // rename
    private static final byte OP_DELETE = 2;  // delete
    private static final byte OP_MKDIR = 3;   // create directory
    private static final byte OP_SET_REPLICATION = 4; // set replication
    //the following two are used only for backward compatibility :
    @Deprecated
    private static final byte OP_DATANODE_ADD = 5;
    @Deprecated
    private static final byte OP_DATANODE_REMOVE = 6;
    private static final byte OP_SET_PERMISSIONS = 7;
    private static final byte OP_SET_OWNER = 8;
    private static final byte OP_CLOSE = 9;    // close after write
    private static final byte OP_SET_GENSTAMP = 10;    // store genstamp
    /* The following two are not used any more. Should be removed once
     * LAST_UPGRADABLE_LAYOUT_VERSION is -17 or newer. */
    private static final byte OP_SET_NS_QUOTA = 11; // set namespace quota
    private static final byte OP_CLEAR_NS_QUOTA = 12; // clear namespace quota
    private static final byte OP_TIMES = 13; // sets mod & access time on a file
    private static final byte OP_SET_QUOTA = 14; // sets name and disk quotas.
    private static final byte OP_GET_DELEGATION_TOKEN = 18; //new delegation token
    private static final byte OP_RENEW_DELEGATION_TOKEN = 19; //renew delegation token
    private static final byte OP_CANCEL_DELEGATION_TOKEN = 20; //cancel delegation token
    private static final byte OP_UPDATE_MASTER_KEY = 21; //update master key

    private static int sizeFlushBuffer = 512 * 1024;

    private ArrayList<EditLogOutputStream> editStreams = null;
    private FSImage fsimage = null;

    // a monotonically increasing counter that represents transactionIds.
    private long txid = 0;

    /*保存了上次的同步事务标识，如果txid比synctxid小，说明还已进行同步，因为是多线程操作*/
    private long synctxid = 0;

    // the time of printing the statistics to the log file.
    private long lastPrintTime;

    // is a sync currently running?
    private boolean isSyncRunning;

    // these are statistics counters.
    private long numTransactions;        // number of transactions
    private long numTransactionsBatchedInSync;
    private long totalTimeTransactions;  // total time for all transactions
    private NameNodeInstrumentation metrics;

    private static class TransactionId {
        long txid;

        TransactionId(long value) {
            this.txid = value;
        }
    }

    // stores the most current transactionId of this thread.
    /*
    *
    * */
    private static final ThreadLocal<TransactionId> MY_TRANSACTION_ID = new ThreadLocal<TransactionId>() {
        protected synchronized TransactionId initialValue() {
            return new TransactionId(Long.MAX_VALUE);
        }
    };

    /**
     * An implementation of the abstract class {@link EditLogOutputStream},
     * which stores edits in a local file.
     */
    static private class EditLogFileOutputStream extends EditLogOutputStream {
        private File file;
        private FileOutputStream fp;    // file stream for storing edit logs
        private FileChannel fc;         // 输出文件对应的文件通道

        /*通过write输出的日志记录会写到缓冲区bufCurrent中，
        * 当bufCurrent中的内容需要写往文件时，
        * EditLogFileOutputStream会交换两个缓冲区。
        * */
        private DataOutputBuffer bufCurrent;  // 日志写入缓冲区
        private DataOutputBuffer bufReady;    // 写文件缓冲区
        static ByteBuffer fill = ByteBuffer.allocateDirect(512); // preallocation

        EditLogFileOutputStream(File name) throws IOException {
            super();
            file = name;
            bufCurrent = new DataOutputBuffer(sizeFlushBuffer);
            bufReady = new DataOutputBuffer(sizeFlushBuffer);
            RandomAccessFile rp = new RandomAccessFile(name, "rw");
            fp = new FileOutputStream(rp.getFD()); // open for append
            fc = rp.getChannel();
            fc.position(fc.size());
        }

        @Override
        String getName() {
            return file.getPath();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void write(int b) throws IOException {
            bufCurrent.write(b);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        void write(byte op, Writable... writables) throws IOException {
            write(op); /*输出操作码*/
            for (Writable w : writables) { /*输出操作参数*/
                w.write(bufCurrent);
            }
        }

        /**
         * Create empty edits logs file.
         * 将文件清空，并在缓冲区bufCurrent写入版本号
         * 再将数据输出到文件
         */
        @Override
        void create() throws IOException {
            fc.truncate(0); /*清空文件现有的内容，fc是文件的文件通道对象*/
            fc.position(0);
            bufCurrent.writeInt(FSConstants.LAYOUT_VERSION);
            setReadyToFlush();
            flush();
        }

        @Override
        public void close() throws IOException {
            // close should have been called after all pending transactions
            // have been flushed & synced.
            int bufSize = bufCurrent.size();
            if (bufSize != 0) {
                throw new IOException("FSEditStream has " + bufSize +
                        " bytes still to be flushed and cannot " +
                        "be closed.");
            }
            bufCurrent.close();
            bufReady.close();

            // remove the last INVALID marker from transaction log.
            fc.truncate(fc.position());
            fp.close();

            bufCurrent = bufReady = null;
        }

        /**
         * All data that has been written to the stream so far will be flushed.
         * New data can be still written to the stream while flushing is performed.
         * 当bufCurrent中的内容需要写往文件时，EditLogFileOutputStream 会交换两个缓冲区。
         * 原来的日志写入缓冲区变成了写文件缓冲区，原来的文件缓冲区变成了日志写入缓冲区
         */
        @Override
        void setReadyToFlush() throws IOException {
            assert bufReady.size() == 0 : "previous data is not flushed yet";
            write(OP_INVALID);// 写入日志文件结束标识 OP_INVALID
            DataOutputBuffer tmp = bufReady;
            bufReady = bufCurrent;/*开始保存要写入文件的日志内容*/
            bufCurrent = tmp;/*交换后，bufCurrent 缓冲区为空，可以立即写入日志*/
        }

        /**
         * Flush ready buffer to persistent store.
         * currentBuffer is not flushed as it accumulates new log records
         * while readyBuffer will be flushed and synced.
         * 实现了输出内存缓冲区中的日志数据斌持久化到磁盘两个目标
         * flush：刷新输出流并强制写出所有缓冲的输出数据，flush只是将输出流缓冲区的数据传递给操作系统，并不保证数据实际写入物理设备，往往只保存在操作系统的内核缓冲区
         * sync：强制将刷新数据写入包含该文件的存储设备中，
         * FileChannel.force方法返回时，与文件相关的内核缓冲区的所有修改，将持久化到磁盘中
         */
        @Override
        protected void flushAndSync() throws IOException {
            preallocate();// preallocate file if necessary
            bufReady.writeTo(fp);// 写数据到文件
            bufReady.reset();// 擦除缓冲区中所有数据
            fc.force(false);// 持久化日志数据到磁盘
            fc.position(fc.position() - 1); // 设置下次执行写操作时的文件位置，以便覆盖日志文件结束标识
        }

        /**
         * Return the size of the current edit log including buffered data.
         */
        @Override
        long length() throws IOException {
            // file size + size of both buffers
            return fc.size() + bufReady.size() + bufCurrent.size();
        }

        // allocate a big chunk of data
        private void preallocate() throws IOException {
            long position = fc.position();
            if (position + 4096 >= fc.size()) {
                FSNamesystem.LOG.debug("Preallocating Edit log, current size " +
                        fc.size());
                long newsize = position + 1024 * 1024; // 1MB
                fill.position(0);
                int written = fc.write(fill, newsize);
                FSNamesystem.LOG.debug("Edit log size is now " + fc.size() +
                        " written " + written + " bytes " +
                        " at offset " + newsize);
            }
        }

        /**
         * Returns the file associated with this stream
         */
        File getFile() {
            return file;
        }
    }

    static class EditLogFileInputStream extends EditLogInputStream {
        private File file;
        private FileInputStream fStream;

        EditLogFileInputStream(File name) throws IOException {
            file = name;
            fStream = new FileInputStream(name);
        }

        @Override
        String getName() {
            return file.getPath();
        }

        @Override
        public int available() throws IOException {
            return fStream.available();
        }

        @Override
        public int read() throws IOException {
            return fStream.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return fStream.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            fStream.close();
        }

        @Override
        long length() throws IOException {
            // file size + size of both buffers
            return file.length();
        }
    }

    FSEditLog(FSImage image) {
        fsimage = image;
        isSyncRunning = false;
        metrics = NameNode.getNameNodeMetrics();
        lastPrintTime = FSNamesystem.now();
    }

    private File getEditFile(StorageDirectory sd) {
        return fsimage.getEditFile(sd);
    }

    private File getEditNewFile(StorageDirectory sd) {
        return fsimage.getEditNewFile(sd);
    }

    private int getNumStorageDirs() {
        int numStorageDirs = 0;
        for (Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext(); it.next())
            numStorageDirs++;
        return numStorageDirs;
    }

    private synchronized int getNumEditStreams() {
        return editStreams == null ? 0 : editStreams.size();
    }

    boolean isOpen() {
        return getNumEditStreams() > 0;
    }

    /**
     * Create empty edit log files.
     * Initialize the output stream for logging.
     *
     * @throws IOException
     */
    public synchronized void open() throws IOException {
        numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;
        if (editStreams == null)
            editStreams = new ArrayList<EditLogOutputStream>();
        for (Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            File eFile = getEditFile(sd);
            try {
                EditLogOutputStream eStream = new EditLogFileOutputStream(eFile);
                editStreams.add(eStream);
            } catch (IOException e) {
                FSNamesystem.LOG.warn("Unable to open edit log file " + eFile);
                // Remove the directory from list of storage directories
                it.remove();
            }
        }
    }

    public synchronized void createEditLogFile(File name) throws IOException {
        EditLogOutputStream eStream = new EditLogFileOutputStream(name);
        eStream.create();
        eStream.close();
    }

    /**
     * Shutdown the file store.
     */
    public synchronized void close() throws IOException {
        while (isSyncRunning) {
            try {
                wait(1000);
            } catch (InterruptedException ie) {
            }
        }
        if (editStreams == null) {
            return;
        }
        printStatistics(true);
        numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

        for (int idx = 0; idx < editStreams.size(); idx++) {
            EditLogOutputStream eStream = editStreams.get(idx);
            try {
                eStream.setReadyToFlush();
                eStream.flush();
                eStream.close();
            } catch (IOException e) {
                processIOError(idx);
                idx--;
            }
        }
        editStreams.clear();
    }

    /**
     * If there is an IO Error on any log operations, remove that
     * directory from the list of directories.
     * If no more directories remain, then exit.
     */
    synchronized void processIOError(int index) {
        if (editStreams == null || editStreams.size() <= 1) {
            FSNamesystem.LOG.fatal(
                    "Fatal Error : All storage directories are inaccessible.");
            Runtime.getRuntime().exit(-1);
        }
        assert (index < getNumStorageDirs());
        assert (getNumStorageDirs() == editStreams.size());

        File parentStorageDir = ((EditLogFileOutputStream) editStreams
                .get(index)).getFile()
                .getParentFile().getParentFile();
        editStreams.remove(index);
        //
        // Invoke the ioerror routine of the fsimage
        //
        fsimage.processIOError(parentStorageDir);
    }

    /**
     * If there is an IO Error on any log operations on storage directory,
     * remove any stream associated with that directory
     */
    synchronized void processIOError(StorageDirectory sd) {
        // Try to remove stream only if one should exist
        if (!sd.getStorageDirType().isOfType(NameNodeDirType.EDITS))
            return;
        if (editStreams == null || editStreams.size() <= 1) {
            FSNamesystem.LOG.fatal(
                    "Fatal Error : All storage directories are inaccessible.");
            Runtime.getRuntime().exit(-1);
        }
        for (int idx = 0; idx < editStreams.size(); idx++) {
            File parentStorageDir = ((EditLogFileOutputStream) editStreams
                    .get(idx)).getFile()
                    .getParentFile().getParentFile();
            if (parentStorageDir.getName().equals(sd.getRoot().getName()))
                editStreams.remove(idx);
        }
    }

    /**
     * The specified streams have IO errors. Remove them from logging
     * new transactions.
     */
    private void processIOError(ArrayList<EditLogOutputStream> errorStreams) {
        if (errorStreams == null) {
            return;                       // nothing to do
        }
        for (EditLogOutputStream eStream : errorStreams) {
            int j = 0;
            int numEditStreams = editStreams.size();
            for (j = 0; j < numEditStreams; j++) {
                if (editStreams.get(j) == eStream) {
                    break;
                }
            }
            if (j == numEditStreams) {
                FSNamesystem.LOG.error("Unable to find sync log on which " +
                        " IO error occured. " +
                        "Fatal Error.");
                Runtime.getRuntime().exit(-1);
            }
            processIOError(j);
        }
        fsimage.incrementCheckpointTime();
    }

    /**
     * check if ANY edits.new log exists
     */
    boolean existsNew() throws IOException {
        for (Iterator<StorageDirectory> it =
             fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext(); ) {
            if (getEditNewFile(it.next()).exists()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Load an edit log, and apply the changes to the in-memory structure
     * This is where we apply edits that we've been writing to disk all
     * along.
     * 大部分实现用于根据日志记录的操作码和操作参数，
     * 调用FSDirectory中的对应方法，修改内存元信息
     */
    static int loadFSEdits(EditLogInputStream edits) throws IOException {
        FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
        FSDirectory fsDir = fsNamesys.dir;
        int numEdits = 0;
        int logVersion = 0;
        String clientName = null;
        String clientMachine = null;
        String path = null;
        int numOpAdd = 0, numOpClose = 0, numOpDelete = 0,
                numOpRename = 0, numOpSetRepl = 0, numOpMkDir = 0,
                numOpSetPerm = 0, numOpSetOwner = 0, numOpSetGenStamp = 0,
                numOpTimes = 0, numOpGetDelegationToken = 0,
                numOpRenewDelegationToken = 0, numOpCancelDelegationToken = 0,
                numOpUpdateMasterKey = 0, numOpOther = 0;

        long startTime = FSNamesystem.now();

        DataInputStream in = new DataInputStream(new BufferedInputStream(edits));
        try {
            // Read log file version. Could be missing.
            in.mark(4);
            // If edits log is greater than 2G, available method will return negative
            // numbers, so we avoid having to call available
            boolean available = true;
            try {
                logVersion = in.readByte();
            } catch (EOFException e) {
                available = false;
            }
            if (available) {
                in.reset();
                logVersion = in.readInt();
                if (logVersion < FSConstants.LAYOUT_VERSION) // future version
                    throw new IOException(
                            "Unexpected version of the file system log file: "
                                    + logVersion + ". Current version = "
                                    + FSConstants.LAYOUT_VERSION + ".");
            }
            assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
                    "Unsupported version " + logVersion;

            while (true) {
                long timestamp = 0;
                long mtime = 0;
                long atime = 0;
                long blockSize = 0;
                byte opcode = -1;
                try {
                    opcode = in.readByte();
                    if (opcode == OP_INVALID) {
                        FSNamesystem.LOG.info("Invalid opcode, reached end of edit log " +
                                "Number of transactions found " + numEdits);
                        break; // no more transactions
                    }
                } catch (EOFException e) {
                    break; // no more transactions
                }
                numEdits++;
                switch (opcode) {
                    case OP_ADD:
                    case OP_CLOSE: {
                        // versions > 0 support per file replication
                        // get name and replication
                        int length = in.readInt();
                        if (-7 == logVersion && length != 3 ||
                                -17 < logVersion && logVersion < -7 && length != 4 ||
                                logVersion <= -17 && length != 5) {
                            throw new IOException("Incorrect data format." +
                                    " logVersion is " + logVersion +
                                    " but writables.length is " +
                                    length + ". ");
                        }
                        path = FSImage.readString(in);
                        short replication = adjustReplication(readShort(in));
                        mtime = readLong(in);
                        if (logVersion <= -17) {
                            atime = readLong(in);
                        }
                        if (logVersion < -7) {
                            blockSize = readLong(in);
                        }
                        // get blocks
                        Block blocks[] = null;
                        if (logVersion <= -14) {
                            blocks = readBlocks(in);
                        } else {
                            BlockTwo oldblk = new BlockTwo();
                            int num = in.readInt();
                            blocks = new Block[num];
                            for (int i = 0; i < num; i++) {
                                oldblk.readFields(in);
                                blocks[i] = new Block(oldblk.blkid, oldblk.len,
                                        Block.GRANDFATHER_GENERATION_STAMP);
                            }
                        }

                        // Older versions of HDFS does not store the block size in inode.
                        // If the file has more than one block, use the size of the
                        // first block as the blocksize. Otherwise use the default
                        // block size.
                        if (-8 <= logVersion && blockSize == 0) {
                            if (blocks.length > 1) {
                                blockSize = blocks[0].getNumBytes();
                            } else {
                                long first = ((blocks.length == 1) ? blocks[0].getNumBytes() : 0);
                                blockSize = Math.max(fsNamesys.getDefaultBlockSize(), first);
                            }
                        }

                        PermissionStatus permissions = fsNamesys.getUpgradePermission();
                        if (logVersion <= -11) {
                            permissions = PermissionStatus.read(in);
                        }

                        // clientname, clientMachine and block locations of last block.
                        if (opcode == OP_ADD && logVersion <= -12) {
                            clientName = FSImage.readString(in);
                            clientMachine = FSImage.readString(in);
                            if (-13 <= logVersion) {
                                readDatanodeDescriptorArray(in);
                            }
                        } else {
                            clientName = "";
                            clientMachine = "";
                        }

                        // The open lease transaction re-creates a file if necessary.
                        // Delete the file if it already exists.
                        if (FSNamesystem.LOG.isDebugEnabled()) {
                            FSNamesystem.LOG.debug(opcode + ": " + path +
                                    " numblocks : " + blocks.length +
                                    " clientHolder " + clientName +
                                    " clientMachine " + clientMachine);
                        }

                        fsDir.unprotectedDelete(path, mtime);

                        // add to the file tree
                        INodeFile node = (INodeFile) fsDir.unprotectedAddFile(
                                path, permissions,
                                blocks, replication,
                                mtime, atime, blockSize);
                        if (opcode == OP_ADD) {
                            numOpAdd++;
                            //
                            // Replace current node with a INodeUnderConstruction.
                            // Recreate in-memory lease record.
                            //
                            INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
                                    node.getLocalNameBytes(),
                                    node.getReplication(),
                                    node.getModificationTime(),
                                    node.getPreferredBlockSize(),
                                    node.getBlocks(),
                                    node.getPermissionStatus(),
                                    clientName,
                                    clientMachine,
                                    null);
                            fsDir.replaceNode(path, node, cons);
                            fsNamesys.leaseManager.addLease(cons.clientName, path);
                        }
                        break;
                    }
                    case OP_SET_REPLICATION: {
                        numOpSetRepl++;
                        path = FSImage.readString(in);
                        short replication = adjustReplication(readShort(in));
                        fsDir.unprotectedSetReplication(path, replication, null);
                        break;
                    }
                    case OP_RENAME: {
                        numOpRename++;
                        int length = in.readInt();
                        if (length != 3) {
                            throw new IOException("Incorrect data format. "
                                    + "Mkdir operation.");
                        }
                        /*读入改名后操作的日志记录内容*/
                        String s = FSImage.readString(in);
                        String d = FSImage.readString(in);
                        timestamp = readLong(in);
                        HdfsFileStatus dinfo = fsDir.getFileInfo(d);
                        /*改名*/
                        fsDir.unprotectedRenameTo(s, d, timestamp);
                        /*修改租约管理器中的记录*/
                        fsNamesys.changeLease(s, d, dinfo);
                        break;
                    }
                    case OP_DELETE: {
                        numOpDelete++;
                        int length = in.readInt();
                        if (length != 2) {
                            throw new IOException("Incorrect data format. "
                                    + "delete operation.");
                        }
                        path = FSImage.readString(in);
                        timestamp = readLong(in);
                        fsDir.unprotectedDelete(path, timestamp);
                        break;
                    }
                    case OP_MKDIR: {
                        numOpMkDir++;
                        PermissionStatus permissions = fsNamesys.getUpgradePermission();
                        int length = in.readInt();
                        if (-17 < logVersion && length != 2 ||
                                logVersion <= -17 && length != 3) {
                            throw new IOException("Incorrect data format. "
                                    + "Mkdir operation.");
                        }
                        path = FSImage.readString(in);
                        timestamp = readLong(in);

                        // The disk format stores atimes for directories as well.
                        // However, currently this is not being updated/used because of
                        // performance reasons.
                        if (logVersion <= -17) {
                            atime = readLong(in);
                        }

                        if (logVersion <= -11) {
                            permissions = PermissionStatus.read(in);
                        }
                        fsDir.unprotectedMkdir(path, permissions, timestamp);
                        break;
                    }
                    case OP_SET_GENSTAMP: {
                        numOpSetGenStamp++;
                        long lw = in.readLong();
                        fsDir.namesystem.setGenerationStamp(lw);
                        break;
                    }
                    case OP_DATANODE_ADD: {
                        numOpOther++;
                        FSImage.DatanodeImage nodeimage = new FSImage.DatanodeImage();
                        nodeimage.readFields(in);
                        //Datnodes are not persistent any more.
                        break;
                    }
                    case OP_DATANODE_REMOVE: {
                        numOpOther++;
                        DatanodeID nodeID = new DatanodeID();
                        nodeID.readFields(in);
                        //Datanodes are not persistent any more.
                        break;
                    }
                    case OP_SET_PERMISSIONS: {
                        numOpSetPerm++;
                        if (logVersion > -11)
                            throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
                        fsDir.unprotectedSetPermission(
                                FSImage.readString(in), FsPermission.read(in));
                        break;
                    }
                    case OP_SET_OWNER: {
                        numOpSetOwner++;
                        if (logVersion > -11)
                            throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
                        fsDir.unprotectedSetOwner(FSImage.readString(in),
                                FSImage.readString_EmptyAsNull(in),
                                FSImage.readString_EmptyAsNull(in));
                        break;
                    }
                    case OP_SET_NS_QUOTA: {
                        if (logVersion > -16) {
                            throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
                        }
                        fsDir.unprotectedSetQuota(FSImage.readString(in),
                                readLongWritable(in),
                                FSConstants.QUOTA_DONT_SET);
                        break;
                    }
                    case OP_CLEAR_NS_QUOTA: {
                        if (logVersion > -16) {
                            throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
                        }
                        fsDir.unprotectedSetQuota(FSImage.readString(in),
                                FSConstants.QUOTA_RESET,
                                FSConstants.QUOTA_DONT_SET);
                        break;
                    }

                    case OP_SET_QUOTA:
                        fsDir.unprotectedSetQuota(FSImage.readString(in),
                                readLongWritable(in),
                                readLongWritable(in));

                        break;

                    case OP_TIMES: {
                        numOpTimes++;
                        int length = in.readInt();
                        if (length != 3) {
                            throw new IOException("Incorrect data format. "
                                    + "times operation.");
                        }
                        path = FSImage.readString(in);
                        mtime = readLong(in);
                        atime = readLong(in);
                        fsDir.unprotectedSetTimes(path, mtime, atime, true);
                        break;
                    }
                    case OP_GET_DELEGATION_TOKEN: {
                        if (logVersion > -19) {
                            throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
                        }
                        numOpGetDelegationToken++;
                        DelegationTokenIdentifier delegationTokenId =
                                new DelegationTokenIdentifier();
                        delegationTokenId.readFields(in);
                        long expiryTime = readLong(in);
                        fsNamesys.getDelegationTokenSecretManager()
                                .addPersistedDelegationToken(delegationTokenId, expiryTime);
                        break;
                    }
                    case OP_RENEW_DELEGATION_TOKEN: {
                        if (logVersion > -19) {
                            throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
                        }
                        numOpRenewDelegationToken++;
                        DelegationTokenIdentifier delegationTokenId =
                                new DelegationTokenIdentifier();
                        delegationTokenId.readFields(in);
                        long expiryTime = readLong(in);
                        fsNamesys.getDelegationTokenSecretManager()
                                .updatePersistedTokenRenewal(delegationTokenId, expiryTime);
                        break;
                    }
                    case OP_CANCEL_DELEGATION_TOKEN: {
                        if (logVersion > -19) {
                            throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
                        }
                        numOpCancelDelegationToken++;
                        DelegationTokenIdentifier delegationTokenId =
                                new DelegationTokenIdentifier();
                        delegationTokenId.readFields(in);
                        fsNamesys.getDelegationTokenSecretManager()
                                .updatePersistedTokenCancellation(delegationTokenId);
                        break;
                    }
                    case OP_UPDATE_MASTER_KEY: {
                        if (logVersion > -19) {
                            throw new IOException("Unexpected opcode " + opcode
                                    + " for version " + logVersion);
                        }
                        numOpUpdateMasterKey++;
                        DelegationKey delegationKey = new DelegationKey();
                        delegationKey.readFields(in);
                        fsNamesys.getDelegationTokenSecretManager().updatePersistedMasterKey(
                                delegationKey);
                        break;
                    }
                    default: {
                        throw new IOException("Never seen opcode " + opcode);
                    }
                }
            }
        } catch (IOException ex) {
            // Failed to load 0.20.203 version edits during upgrade. This version has
            // conflicting opcodes with the later releases. The editlog must be
            // emptied by restarting the namenode, before proceeding with the upgrade.
            if (Storage.is203LayoutVersion(logVersion) &&
                    logVersion != FSConstants.LAYOUT_VERSION) {
                String msg = "During upgrade, failed to load the editlog version " +
                        logVersion + " from release 0.20.203. Please go back to the old " +
                        " release and restart the namenode. This empties the editlog " +
                        " and saves the namespace. Resume the upgrade after this step.";
                throw new IOException(msg, ex);
            } else {
                throw ex;
            }

        } finally {
            in.close();
        }
        FSImage.LOG.info("Edits file " + edits.getName()
                + " of size " + edits.length() + " edits # " + numEdits
                + " loaded in " + (FSNamesystem.now() - startTime) / 1000 + " seconds.");

        if (FSImage.LOG.isDebugEnabled()) {
            FSImage.LOG.debug("numOpAdd = " + numOpAdd + " numOpClose = " + numOpClose
                    + " numOpDelete = " + numOpDelete + " numOpRename = " + numOpRename
                    + " numOpSetRepl = " + numOpSetRepl + " numOpMkDir = " + numOpMkDir
                    + " numOpSetPerm = " + numOpSetPerm
                    + " numOpSetOwner = " + numOpSetOwner
                    + " numOpSetGenStamp = " + numOpSetGenStamp
                    + " numOpTimes = " + numOpTimes
                    + " numOpGetDelegationToken = " + numOpGetDelegationToken
                    + " numOpRenewDelegationToken = " + numOpRenewDelegationToken
                    + " numOpCancelDelegationToken = " + numOpCancelDelegationToken
                    + " numOpUpdateMasterKey = " + numOpUpdateMasterKey
                    + " numOpOther = " + numOpOther);
        }

        if (logVersion != FSConstants.LAYOUT_VERSION) // other version
            numEdits++; // save this image asap
        return numEdits;
    }

    // a place holder for reading a long
    private static final LongWritable longWritable = new LongWritable();

    /**
     * Read an integer from an input stream
     */
    private static long readLongWritable(DataInputStream in) throws IOException {
        synchronized (longWritable) {
            longWritable.readFields(in);
            return longWritable.get();
        }
    }

    static short adjustReplication(short replication) {
        FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
        short minReplication = fsNamesys.getMinReplication();
        if (replication < minReplication) {
            replication = minReplication;
        }
        short maxReplication = fsNamesys.getMaxReplication();
        if (replication > maxReplication) {
            replication = maxReplication;
        }
        return replication;
    }

    /**
     * Write an operation to the edit log. Do not sync to persistent
     * store yet.
     */
    private synchronized void logEdit(byte op, Writable... writables) {
        assert this.getNumEditStreams() > 0 : "no editlog streams";
        long start = FSNamesystem.now();
        for (int idx = 0; idx < editStreams.size(); idx++) {
            EditLogOutputStream eStream = editStreams.get(idx);
            try {
                eStream.write(op, writables);
            } catch (IOException ie) {
                processIOError(idx);
                // processIOError will remove the idx's stream
                // from the editStreams collection, so we need to update idx
                idx--;
            }
        }
        // get a new transactionId
        txid++;

        //
        // record the transactionId when new data was written to the edits log
        //
        TransactionId id = MY_TRANSACTION_ID.get();
        id.txid = txid;

        // update statistics
        long end = FSNamesystem.now();
        numTransactions++;
        totalTimeTransactions += (end - start);
        if (metrics != null) // Metrics is non-null only when used inside name node
            metrics.addTransaction(end - start);
    }

    //
    // Sync all modifications done by this thread.
    // 当调用完【logEdit】方法后，就会使用logSync()同步日志修改
    //
    void logSync() throws IOException {
        ArrayList<EditLogOutputStream> errorStreams = null;
        long syncStart = 0;

        // Fetch the transactionId of this thread.
        long mytxid = MY_TRANSACTION_ID.get().txid;

        final int numEditStreams;
        synchronized (this) {
            numEditStreams = editStreams.size();
            assert numEditStreams > 0 : "no editlog streams";
            printStatistics(false);

            // 有其他线程正在执行日志同步操作
            /* namenode 是一个多线程应用，
             * 如果饭前事务标识大于已处理的事务标识
             * 需要等待其他线程执行完毕
             * */
            while (mytxid > synctxid && isSyncRunning) {
                try {
                    wait(1000);
                } catch (InterruptedException ignored) {

                }
            }

            //
            // If this transaction was already flushed, then nothing to do
            // 说明日志已经被其他线程同步，返回
            //
            if (mytxid <= synctxid) {
                numTransactionsBatchedInSync++;
                if (metrics != null) // Metrics is non-null only when used inside name node
                    metrics.incrTransactionsBatchedInSync();
                return;
            }

            // 同步由当前线程执行
            syncStart = txid;
            isSyncRunning = true;

            // 交换缓冲区
            for (int idx = 0; idx < numEditStreams; idx++) {
                editStreams.get(idx).setReadyToFlush();
            }
        }

        // 执行同步操作
        long start = FSNamesystem.now();
        for (int idx = 0; idx < numEditStreams; idx++) {
            EditLogOutputStream eStream = editStreams.get(idx);
            try {
                eStream.flush();
            } catch (IOException ie) {
                //
                // remember the streams that encountered an error.
                //
                if (errorStreams == null) {
                    errorStreams = new ArrayList<>(1);
                }
                errorStreams.add(eStream);
                FSNamesystem.LOG.error("Unable to sync edit log. " +
                        "Fatal Error.");
            }
        }
        long elapsed = FSNamesystem.now() - start;

        synchronized (this) {
            processIOError(errorStreams);
            synctxid = syncStart;
            isSyncRunning = false;
            this.notifyAll();
        }

        if (metrics != null) // Metrics is non-null only when used inside name node
            metrics.addSync(elapsed);
    }

    //
    // print statistics every 1 minute.
    //
    private void printStatistics(boolean force) {
        long now = FSNamesystem.now();
        if (lastPrintTime + 60000 > now && !force) {
            return;
        }
        if (editStreams == null || editStreams.size() == 0) {
            return;
        }
        lastPrintTime = now;
        StringBuilder buf = new StringBuilder();
        buf.append("Number of transactions: ");
        buf.append(numTransactions);
        buf.append(" Total time for transactions(ms): ");
        buf.append(totalTimeTransactions);
        buf.append("Number of transactions batched in Syncs: ");
        buf.append(numTransactionsBatchedInSync);
        buf.append(" Number of syncs: ");
        buf.append(editStreams.get(0).getNumSync());
        buf.append(" SyncTimes(ms): ");

        for (EditLogOutputStream eStream : editStreams) {
            buf.append(eStream.getTotalSyncTime());
            buf.append(" ");
        }
        FSNamesystem.LOG.info(buf);
    }

    /**
     * Add open lease record to edit log.
     * Records the block locations of the last block.
     */
    public void logOpenFile(String path, INodeFileUnderConstruction newNode)
            throws IOException {

        UTF8 nameReplicationPair[] = new UTF8[]{
                new UTF8(path),
                FSEditLog.toLogReplication(newNode.getReplication()),/*复本数*/
                FSEditLog.toLogLong(newNode.getModificationTime()),/*修改时间*/
                FSEditLog.toLogLong(newNode.getAccessTime()),/*访问时间*/
                FSEditLog.toLogLong(newNode.getPreferredBlockSize())};

        logEdit(OP_ADD,
                new ArrayWritable(UTF8.class, nameReplicationPair),
                new ArrayWritable(Block.class, newNode.getBlocks()),
                newNode.getPermissionStatus(),
                new UTF8(newNode.getClientName()),
                new UTF8(newNode.getClientMachine()));
    }

    /**
     * Add close lease record to edit log.
     */
    public void logCloseFile(String path, INodeFile newNode) {
        UTF8 nameReplicationPair[] = new UTF8[]{
                new UTF8(path),
                FSEditLog.toLogReplication(newNode.getReplication()),
                FSEditLog.toLogLong(newNode.getModificationTime()),
                FSEditLog.toLogLong(newNode.getAccessTime()),
                FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
        logEdit(OP_CLOSE,
                new ArrayWritable(UTF8.class, nameReplicationPair),
                new ArrayWritable(Block.class, newNode.getBlocks()),
                newNode.getPermissionStatus());
    }

    /**
     * Add create directory record to edit log
     */
    public void logMkDir(String path, INode newNode) {
        UTF8 info[] = new UTF8[]{
                new UTF8(path),
                FSEditLog.toLogLong(newNode.getModificationTime()),
                FSEditLog.toLogLong(newNode.getAccessTime())
        };
        logEdit(OP_MKDIR, new ArrayWritable(UTF8.class, info),
                newNode.getPermissionStatus());
    }

    /**
     * Add rename record to edit log
     * TODO: use String parameters until just before writing to disk
     */
    void logRename(String src, String dst, long timestamp) {
        UTF8 info[] = new UTF8[]{
                new UTF8(src),
                new UTF8(dst),
                FSEditLog.toLogLong(timestamp)};
        logEdit(OP_RENAME, new ArrayWritable(UTF8.class, info));
    }

    /**
     * Add set replication record to edit log
     */
    void logSetReplication(String src, short replication) {
        logEdit(OP_SET_REPLICATION,
                new UTF8(src),
                FSEditLog.toLogReplication(replication));
    }

    /**
     * Add set namespace quota record to edit log
     *
     * @param src     the string representation of the path to a directory
     * @param nsQuota the directory size limit
     */
    void logSetQuota(String src, long nsQuota, long dsQuota) {
        logEdit(OP_SET_QUOTA, new UTF8(src),
                new LongWritable(nsQuota), new LongWritable(dsQuota));
    }

    /**
     * Add set permissions record to edit log
     */
    void logSetPermissions(String src, FsPermission permissions) {
        logEdit(OP_SET_PERMISSIONS, new UTF8(src), permissions);
    }

    /**
     * Add set owner record to edit log
     */
    void logSetOwner(String src, String username, String groupname) {
        UTF8 u = new UTF8(username == null ? "" : username);
        UTF8 g = new UTF8(groupname == null ? "" : groupname);
        logEdit(OP_SET_OWNER, new UTF8(src), u, g);
    }

    /**
     * Add delete file record to edit log
     */
    void logDelete(String src, long timestamp) {
        UTF8 info[] = new UTF8[]{
                new UTF8(src),
                FSEditLog.toLogLong(timestamp)};
        logEdit(OP_DELETE, new ArrayWritable(UTF8.class, info));
    }

    /**
     * Add generation stamp record to edit log
     */
    void logGenerationStamp(long genstamp) {
        logEdit(OP_SET_GENSTAMP, new LongWritable(genstamp));
    }

    /**
     * Add access time record to edit log
     */
    void logTimes(String src, long mtime, long atime) {
        UTF8 info[] = new UTF8[]{
                new UTF8(src),
                FSEditLog.toLogLong(mtime),
                FSEditLog.toLogLong(atime)};
        logEdit(OP_TIMES, new ArrayWritable(UTF8.class, info));
    }

    /**
     * log delegation token to edit log
     *
     * @param id         DelegationTokenIdentifier
     * @param expiryTime of the token
     * @return
     */
    void logGetDelegationToken(DelegationTokenIdentifier id,
                               long expiryTime) {
        logEdit(OP_GET_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
    }

    void logRenewDelegationToken(DelegationTokenIdentifier id,
                                 long expiryTime) {
        logEdit(OP_RENEW_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
    }

    void logCancelDelegationToken(DelegationTokenIdentifier id) {
        logEdit(OP_CANCEL_DELEGATION_TOKEN, id);
    }

    void logUpdateMasterKey(DelegationKey key) {
        logEdit(OP_UPDATE_MASTER_KEY, key);
    }

    static private UTF8 toLogReplication(short replication) {
        return new UTF8(Short.toString(replication));
    }

    static private UTF8 toLogLong(long timestamp) {
        return new UTF8(Long.toString(timestamp));
    }

    /**
     * Return the size of the current EditLog
     */
    synchronized long getEditLogSize() throws IOException {
        assert (getNumStorageDirs() == editStreams.size());
        long size = 0;
        for (int idx = 0; idx < editStreams.size(); idx++) {
            long curSize = editStreams.get(idx).length();
            assert (size == 0 || size == curSize) : "All streams must be the same";
            size = curSize;
        }
        return size;
    }

    /**
     * Closes the current edit log and opens edits.new.
     * Returns the lastModified time of the edits log.
     */
    synchronized void rollEditLog() throws IOException {
        //
        // If edits.new already exists in some directory, verify it
        // exists in all directories.
        //
        /*如果 edits.new存在，则立即返回*/
        if (existsNew()) {
            for (Iterator<StorageDirectory> it =
                 fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext(); ) {
                File editsNew = getEditNewFile(it.next());
                if (!editsNew.exists()) {
                    throw new IOException("Inconsistent existance of edits.new " +
                            editsNew);
                }
            }
            return; // nothing to do, edits.new exists!
        }
        /*关闭当前 edits 的日志文件输出*/
        close();                     // close existing edit log

        //
        // Open edits.new
        /* 打开新的日志文件 edits.new */
        //
        for (Iterator<StorageDirectory> it =
             fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            try {
                EditLogFileOutputStream eStream =
                        new EditLogFileOutputStream(getEditNewFile(sd));
                eStream.create();
                editStreams.add(eStream);
            } catch (IOException e) {
                // remove stream and this storage directory from list
                processIOError(sd);
                it.remove();
            }
        }
    }

    /**
     * Removes the old edit log and renamed edits.new as edits.
     * Reopens the edits file.
     * <p>
     * 该方法执行的时候，日志记录可保存在缓冲区bufCurrent，但不会写日志。因为被阻塞了
     */
    synchronized void purgeEditLog() throws IOException {
        //
        // If edits.new does not exists, then return error.
        //
        if (!existsNew()) {
            throw new IOException("Attempt to purge edit log " +
                    "but edits.new does not exist.");
        }
        close();/*关闭到“edit.new”文件的输出*/

        //
        // Delete edits and rename edits.new to edits.
        //
        for (Iterator<StorageDirectory> it =
             fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext(); ) {
            StorageDirectory sd = it.next();
            if (!getEditNewFile(sd).renameTo(getEditFile(sd))) {
                //
                // renameTo() fails on Windows if the destination
                // file exists.
                //
                getEditFile(sd).delete();
                if (!getEditNewFile(sd).renameTo(getEditFile(sd))) {
                    // Should we also remove from edits
                    it.remove();
                }
            }
        }
        //
        // Reopen all the edits logs.
        //
        open();/*重新打开编辑日志*/
    }

    /**
     * Return the name of the edit file
     */
    synchronized File getFsEditName() throws IOException {
        StorageDirectory sd = null;
        for (Iterator<StorageDirectory> it =
             fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext(); )
            sd = it.next();
        return getEditFile(sd);
    }

    /**
     * Returns the timestamp of the edit log
     */
    synchronized long getFsEditTime() {
        Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
        if (it.hasNext())
            return getEditFile(it.next()).lastModified();
        return 0;
    }

    // sets the initial capacity of the flush buffer.
    static void setBufferCapacity(int size) {
        sizeFlushBuffer = size;
    }

    /**
     * A class to read in blocks stored in the old format. The only two
     * fields in the block were blockid and length.
     */
    static class BlockTwo implements Writable {
        long blkid;
        long len;

        static {                                      // register a ctor
            WritableFactories.setFactory
                    (BlockTwo.class,
                            new WritableFactory() {
                                public Writable newInstance() {
                                    return new BlockTwo();
                                }
                            });
        }


        BlockTwo() {
            blkid = 0;
            len = 0;
        }

        /////////////////////////////////////
        // Writable
        /////////////////////////////////////
        public void write(DataOutput out) throws IOException {
            out.writeLong(blkid);
            out.writeLong(len);
        }

        public void readFields(DataInput in) throws IOException {
            this.blkid = in.readLong();
            this.len = in.readLong();
        }
    }

    /**
     * This method is defined for compatibility reason.
     */
    static private DatanodeDescriptor[] readDatanodeDescriptorArray(DataInput in
    ) throws IOException {
        DatanodeDescriptor[] locations = new DatanodeDescriptor[in.readInt()];
        for (int i = 0; i < locations.length; i++) {
            locations[i] = new DatanodeDescriptor();
            locations[i].readFieldsFromFSEditLog(in);
        }
        return locations;
    }

    static private short readShort(DataInputStream in) throws IOException {
        return Short.parseShort(FSImage.readString(in));
    }

    static private long readLong(DataInputStream in) throws IOException {
        return Long.parseLong(FSImage.readString(in));
    }

    static private Block[] readBlocks(DataInputStream in) throws IOException {
        int numBlocks = in.readInt();
        Block[] blocks = new Block[numBlocks];
        for (int i = 0; i < numBlocks; i++) {
            blocks[i] = new Block();
            blocks[i].readFields(in);
        }
        return blocks;
    }
}
