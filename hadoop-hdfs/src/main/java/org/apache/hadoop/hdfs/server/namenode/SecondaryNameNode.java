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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.source.JvmMetricsSource;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Krb5AndCertsSslSocketConnector;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**********************************************************
 * The Secondary NameNode is a helper to the primary NameNode.
 * The Secondary is responsible for supporting periodic checkpoints 
 * of the HDFS metadata. The current design allows only one Secondary
 * NameNode per HDFs cluster.
 *
 * The Secondary NameNode is a daemon that periodically wakes
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * The Secondary NameNode uses the ClientProtocol to talk to the
 * primary NameNode.
 *
 **********************************************************/
public class SecondaryNameNode implements Runnable {

    static {
        Configuration.addDefaultResource("src/main/resources/hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
    }

    public static final Log LOG = LogFactory.getLog(SecondaryNameNode.class.getName());

    private String fsName;
    private CheckpointStorage checkpointImage;

    private NamenodeProtocol namenode;
    private Configuration conf;
    private InetSocketAddress nameNodeAddr;
    private volatile boolean shouldRun;
    private HttpServer infoServer;
    private int infoPort;
    private int imagePort;
    private String infoBindAddress;

    private Collection<File> checkpointDirs;
    private Collection<File> checkpointEditsDirs;
    private long checkpointPeriod;    // in seconds
    private long checkpointSize;    // size (in MB) of current Edit Log

    /**
     * Utility class to facilitate junit test error simulation.
     */
    static class ErrorSimulator {
        private static boolean[] simulation = null; // error simulation events

        static void initializeErrorSimulationEvent(int numberOfEvents) {
            simulation = new boolean[numberOfEvents];
            for (int i = 0; i < numberOfEvents; i++) {
                simulation[i] = false;
            }
        }

        static boolean getErrorSimulation(int index) {
            if (simulation == null)
                return false;
            assert (index < simulation.length);
            return simulation[index];
        }

        static void setErrorSimulation(int index) {
            assert (index < simulation.length);
            simulation[index] = true;
        }

        static void clearErrorSimulation(int index) {
            assert (index < simulation.length);
            simulation[index] = false;
        }
    }

    FSImage getFSImage() {
        return checkpointImage;
    }

    /**
     * Create a connection to the primary namenode.
     */
    public SecondaryNameNode(Configuration conf) throws IOException {
        try {
            initialize(conf);
        } catch (IOException e) {
            shutdown();
            throw e;
        }
    }

    @SuppressWarnings("deprecation")
    public static InetSocketAddress getHttpAddress(Configuration conf) {
        String infoAddr = NetUtils.getServerAddress(conf,
                "dfs.secondary.info.bindAddress", "dfs.secondary.info.port",
                "dfs.secondary.http.address");
        return NetUtils.createSocketAddr(infoAddr);
    }

    /**
     * Initialize SecondaryNameNode.
     */
    private void initialize(final Configuration conf) throws IOException {
        final InetSocketAddress infoSocAddr = getHttpAddress(conf);
        infoBindAddress = infoSocAddr.getHostName();
        if (UserGroupInformation.isSecurityEnabled()) {
            SecurityUtil.login(conf,
                    DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY,
                    DFSConfigKeys.DFS_SECONDARY_NAMENODE_USER_NAME_KEY,
                    infoBindAddress);
        }

        // initiate Java VM metrics
        JvmMetricsSource.create("SecondaryNameNode", conf.get("session.id"));

        // Create connection to the namenode.
        shouldRun = true;
        nameNodeAddr = NameNode.getServiceAddress(conf, true);

        this.conf = conf;
        this.namenode = (NamenodeProtocol) RPC.waitForProxy(NamenodeProtocol.class,
                NamenodeProtocol.versionID, nameNodeAddr, conf);

        // initialize checkpoint directories
        fsName = getInfoServer();
        checkpointDirs = FSImage.getCheckpointDirs(conf, "/tmp/hadoop/dfs/namesecondary");
        checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, "/tmp/hadoop/dfs/namesecondary");
        checkpointImage = new CheckpointStorage();
        checkpointImage.recoverCreate(checkpointDirs, checkpointEditsDirs);

        // Initialize other scheduling parameters from the configuration
        checkpointPeriod = conf.getLong("fs.checkpoint.period", 3600);
        checkpointSize = conf.getLong("fs.checkpoint.size", 4194304);

        // initialize the webserver for uploading files.
        // Kerberized SSL servers must be run from the host principal...
        UserGroupInformation httpUGI =
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                        SecurityUtil.getServerPrincipal(conf.get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY), infoBindAddress),
                        conf.get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY));
        try {
            infoServer = httpUGI.doAs((PrivilegedExceptionAction<HttpServer>) () -> {
                LOG.info("Starting web server as: " + UserGroupInformation.getCurrentUser().getUserName());

                int tmpInfoPort = infoSocAddr.getPort();
                infoServer = new HttpServer("secondary", infoBindAddress, tmpInfoPort,
                        tmpInfoPort == 0, conf,
                        SecurityUtil.getAdminAcls(conf, DFSConfigKeys.DFS_ADMIN));

                if (UserGroupInformation.isSecurityEnabled()) {
                    System.setProperty("https.cipherSuites",
                            Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.get(0));
                    InetSocketAddress secInfoSocAddr =
                            NetUtils.createSocketAddr(infoBindAddress + ":" + conf.get(
                                    "dfs.secondary.https.port", infoBindAddress + ":" + 0));
                    imagePort = secInfoSocAddr.getPort();
                    infoServer.addSslListener(secInfoSocAddr, conf, false, true);
                }

                infoServer.setAttribute("name.system.image", checkpointImage);
                infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
                infoServer.addInternalServlet("getimage", "/getimage",
                        GetImageServlet.class, true);
                infoServer.start();
                return infoServer;
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Web server init done");
        // The web-server port can be ephemeral... ensure we have the correct info

        infoPort = infoServer.getPort();
        if (!UserGroupInformation.isSecurityEnabled())
            imagePort = infoPort;

        conf.set("dfs.secondary.http.address", infoBindAddress + ":" + infoPort);
        LOG.info("Secondary Web-server up at: " + infoBindAddress + ":" + infoPort);
        LOG.info("Secondary image servlet up at: " + infoBindAddress + ":" + imagePort);
        LOG.warn("Checkpoint Period   :" + checkpointPeriod + " secs " +
                "(" + checkpointPeriod / 60 + " min)");
        LOG.warn("Log Size Trigger    :" + checkpointSize + " bytes " +
                "(" + checkpointSize / 1024 + " KB)");
    }

    /**
     * Shut down this instance of the datanode.
     * Returns only after shutdown is complete.
     */
    public void shutdown() {
        shouldRun = false;
        try {
            if (infoServer != null) infoServer.stop();
        } catch (Exception e) {
            LOG.warn("Exception shutting down SecondaryNameNode", e);
        }
        try {
            if (checkpointImage != null) checkpointImage.close();
        } catch (IOException e) {
            LOG.warn(StringUtils.stringifyException(e));
        }
    }

    public void run() {
        if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation ugi;
            try {
                ugi = UserGroupInformation.getLoginUser();
            } catch (IOException e) {
                LOG.error(StringUtils.stringifyException(e));
                e.printStackTrace();
                Runtime.getRuntime().exit(-1);
                return;
            }
            ugi.doAs((PrivilegedAction<Object>) () -> {
                doWork();
                return null;
            });
        } else {
            doWork();
        }
    }

    //
    // The main work loop
    //
    private void doWork() {
        //
        // Poll the Namenode (once every 5 minutes) to find the size of the
        // pending edit log.
        /* 默认最长5分钟一次检测编辑日志的长度 */
        //
        long period = 5 * 60;              // 5 minutes
        long lastCheckpointTime = 0;
        if (checkpointPeriod < period) {
            period = checkpointPeriod;
        }

        while (shouldRun) {
            try {
                Thread.sleep(1000 * period);
            } catch (InterruptedException ie) {
                // do nothing
            }
            if (!shouldRun) {
                break;
            }
            try {
                // We may have lost our ticket since last checkpoint, log in again, just in case
                if (UserGroupInformation.isSecurityEnabled())
                    UserGroupInformation.getCurrentUser().reloginFromKeytab();

                long now = System.currentTimeMillis();

                long size = namenode.getEditLogSize();/*获得名字节点编辑日志大小*/
                if (size >= checkpointSize
                        || now >= lastCheckpointTime + 1000 * checkpointPeriod) {
                    doCheckpoint();
                    lastCheckpointTime = now;
                }
            } catch (IOException e) {
                LOG.error("Exception in doCheckpoint: ");
                LOG.error(StringUtils.stringifyException(e));
                e.printStackTrace();
            } catch (Throwable e) {
                LOG.error("Throwable Exception in doCheckpoint: ");
                LOG.error(StringUtils.stringifyException(e));
                e.printStackTrace();
                Runtime.getRuntime().exit(-1);
            }
        }
    }

    /**
     * Download <code>fsimage</code> and <code>edits</code>
     * files from the name-node.
     *
     * @throws IOException
     */
    private void downloadCheckpointFiles(final CheckpointSignature sig) throws IOException {
        try {
            UserGroupInformation.getCurrentUser().doAs((PrivilegedExceptionAction<Void>) () -> {
                checkpointImage.cTime = sig.cTime;
                checkpointImage.checkpointTime = sig.checkpointTime;

                // get fsimage
                String field = "getimage=1";
                File[] srcNames = checkpointImage.getImageFiles();
                assert srcNames.length > 0 : "No checkpoint targets.";
                TransferFsImage.getFileClient(fsName, field, srcNames);
                LOG.info("Downloaded file " + srcNames[0].getName() + " size " +
                        srcNames[0].length() + " bytes.");

                // get edits file
                field = "getedit=1";
                srcNames = checkpointImage.getEditsFiles();
                assert srcNames.length > 0 : "No checkpoint targets.";
                TransferFsImage.getFileClient(fsName, field, srcNames);
                LOG.info("Downloaded file " + srcNames[0].getName() + " size " + srcNames[0].length() + " bytes.");

                checkpointImage.checkpointUploadDone();
                return null;
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Copy the new fsimage into the NameNode
     * http请求的url中携带了参数 putimage ，这是一个特殊的请求，
     * 当NameNoe处理这个特殊请求时，会使用TransferFsImage.getFileClient()创建一个到SecondaryNameNode的http连接
     * 获取 fsimage 文件。
     * <p>
     * <p></p>
     * <p>
     * 当该方法返回时，NameNode已经下载了最新的命名空间镜像。
     */
    private void putFSImage(CheckpointSignature sig) throws IOException {
        String field = "putimage=1&port=" + imagePort +
                "&machine=" + infoBindAddress +
                "&token=" + sig.toString();
        LOG.info("Posted URL " + fsName + field);
        TransferFsImage.getFileClient(fsName, field, null);
    }

    /**
     * Returns the Jetty server that the Namenode is listening on.
     */
    private String getInfoServer() throws IOException {
        URI fsName = FileSystem.getDefaultUri(conf);
        if (!"hdfs".equals(fsName.getScheme())) {
            throw new IOException("This is not a DFS");
        }
        String infoAddr = NameNode.getInfoServer(conf);
        LOG.debug("infoAddr = " + infoAddr);
        return infoAddr;
    }

    /**
     * Create a new checkpoint
     */
    void doCheckpoint() throws IOException {

        // Do the required initialization of the merge work area.
        /*第二名字节点 初始化工作区*/
        startCheckpoint();

        // Tell the namenode to start logging transactions in a new edit file
        // Retuns a token that would be used to upload the merged image.
        /*通知NameNode进行edits滚动（停止编辑当前日志，建立新的编辑日志），返回值用于合并后的检查点上传*/
        CheckpointSignature sig = namenode.rollEditLog();

        // error simulation code for junit test
        if (ErrorSimulator.getErrorSimulation(0)) {
            throw new IOException("Simulating error0 " +
                    "after creating edits.new");
        }

        /*
        * 通过HTTP协议下载 fsimage edits 到本地工作区
        *
        * http://<名字节点地址>:<名字节点的HTTP端口>/getimage?getimage=1
        * http://<名字节点地址>:<名字节点的HTTP端口>/getimage?getedit=1
        *
        * */
        downloadCheckpointFiles(sig);   //

        doMerge(sig);                   // 进行合并，合并后的内存元数据会保存到第二名字节点的磁盘上，形成新的镜像文件。

        //
        // Upload the new image into the NameNode. Then tell the Namenode
        // to make this new uploaded image as the most current image.
        //
        putFSImage(sig); /*使用HTTP，上传新的命名空间镜像到NameNode*/

        // error simulation code for junit test
        if (ErrorSimulator.getErrorSimulation(1)) {
            throw new IOException("Simulating error1 " +
                    "after uploading new image to NameNode");
        }
        /*NameNode已经下载完最新的命名空间镜像*/
        namenode.rollFsImage();/*通知NameNode，命名空间镜像上传完毕*/
        checkpointImage.endCheckpoint();/*清理本地工作区*/

        LOG.warn("Checkpoint done. New Image Size: "
                + checkpointImage.getFsImageName().length());
    }

    private void startCheckpoint() throws IOException {
        checkpointImage.unlockAll();
        checkpointImage.getEditLog().close();
        /*检查第二名字节点管理的磁盘目录文件结构，并执行可能的文件结构恢复*/
        /*和名字节点一样，产生的检查点的过程使用临时目录，既保存数据，又维护状态*/
        /*
        * 另外，第二名字节点的根目录结构和NameNode正常工作时的状态基本一致，因为CheckpointStorage类就是继承自FSImage
        * 同时，如果NameNode发生严重的故障并且没有用于恢复的备份，那么可以拷贝第二名字节点的“current”目录中的数据
        * */
        checkpointImage.recoverCreate(checkpointDirs, checkpointEditsDirs);
        checkpointImage.startCheckpoint();
    }

    /**
     * Merge downloaded image and edits and write the new image into
     * current storage directory.
     */

    private void doMerge(CheckpointSignature sig) throws IOException {
        FSNamesystem namesystem = new FSNamesystem(checkpointImage, conf);
        assert namesystem.dir.fsImage == checkpointImage;
        checkpointImage.doMerge(sig);
    }

    /**
     * @param argv The parameters passed to this program.
     * @return 0 on success, non zero on error.
     * @throws Exception if the filesystem does not exist.
     */
    private int processArgs(String[] argv) throws Exception {

        if (argv.length < 1) {
            printUsage("");
            return -1;
        }

        int exitCode = -1;
        int i = 0;
        String cmd = argv[i++];

        //
        // verify that we have enough command line parameters
        //
        if ("-geteditsize".equals(cmd)) {
            if (argv.length != 1) {
                printUsage(cmd);
                return exitCode;
            }
        } else if ("-checkpoint".equals(cmd)) {
            if (argv.length != 1 && argv.length != 2) {
                printUsage(cmd);
                return exitCode;
            }
            if (argv.length == 2 && !"force".equals(argv[i])) {
                printUsage(cmd);
                return exitCode;
            }
        }

        exitCode = 0;
        try {
            if ("-checkpoint".equals(cmd)) {
                long size = namenode.getEditLogSize();
                if (size >= checkpointSize ||
                        argv.length == 2 && "force".equals(argv[i])) {
                    doCheckpoint();
                } else {
                    System.err.println("EditLog size " + size + " bytes is " +
                            "smaller than configured checkpoint " +
                            "size " + checkpointSize + " bytes.");
                    System.err.println("Skipping checkpoint.");
                }
            } else if ("-geteditsize".equals(cmd)) {
                long size = namenode.getEditLogSize();
                System.out.println("EditLog size is " + size + " bytes");
            } else {
                exitCode = -1;
                LOG.error(cmd.substring(1) + ": Unknown command");
                printUsage("");
            }
        } catch (RemoteException e) {
            //
            // This is a error returned by hadoop server. Print
            // out the first line of the error mesage, ignore the stack trace.
            exitCode = -1;
            try {
                String[] content;
                content = e.getLocalizedMessage().split("\n");
                LOG.error(cmd.substring(1) + ": "
                        + content[0]);
            } catch (Exception ex) {
                LOG.error(cmd.substring(1) + ": "
                        + ex.getLocalizedMessage());
            }
        } catch (IOException e) {
            //
            // IO exception encountered locally.
            //
            exitCode = -1;
            LOG.error(cmd.substring(1) + ": "
                    + e.getLocalizedMessage());
        } finally {
            // Does the RPC connection need to be closed?
        }
        return exitCode;
    }

    /**
     * Displays format of commands.
     *
     * @param cmd The command that is being executed.
     */
    private void printUsage(String cmd) {
        if ("-geteditsize".equals(cmd)) {
            System.err.println("Usage: java SecondaryNameNode"
                    + " [-geteditsize]");
        } else if ("-checkpoint".equals(cmd)) {
            System.err.println("Usage: java SecondaryNameNode"
                    + " [-checkpoint [force]]");
        } else {
            System.err.println("Usage: java SecondaryNameNode " +
                    "[-checkpoint [force]] " +
                    "[-geteditsize] ");
        }
    }

    /**
     * main() has some simple utility methods.
     *
     * @param argv Command line parameters.
     * @throws Exception if the filesystem does not exist.
     */
    public static void main(String[] argv) throws Exception {
        StringUtils.startupShutdownMessage(SecondaryNameNode.class, argv, LOG);
        Configuration tconf = new Configuration();
        if (argv.length >= 1) {
            SecondaryNameNode secondary = new SecondaryNameNode(tconf);
            int ret = secondary.processArgs(argv);
            System.exit(ret);
        }

        // Create a never ending deamon
        Daemon checkpointThread = new Daemon(new SecondaryNameNode(tconf));
        checkpointThread.start();
    }

    /**
     * 由于FSImage已经实现了第二名字节点合并命名空间和编辑日志的功能，
     * 并被CheckpointStorage复用，降低了SecondaryNameNode的复杂性
     */
    static class CheckpointStorage extends FSImage {
        /**
         */
        CheckpointStorage() throws IOException {
            super();
        }

        @Override
        public boolean isConversionNeeded(StorageDirectory sd) {
            return false;
        }

        /**
         * Analyze checkpoint directories.
         * Create directories if they do not exist.
         * Recover from an unsuccessful checkpoint is necessary.
         *
         * @param dataDirs
         * @param editsDirs
         * @throws IOException
         */
        void recoverCreate(Collection<File> dataDirs, Collection<File> editsDirs) throws IOException {
            Collection<File> tempDataDirs = new ArrayList<File>(dataDirs);
            Collection<File> tempEditsDirs = new ArrayList<File>(editsDirs);
            this.storageDirs = new ArrayList<StorageDirectory>();
            setStorageDirectories(tempDataDirs, tempEditsDirs);
            for (Iterator<StorageDirectory> it =
                 dirIterator(); it.hasNext(); ) {
                StorageDirectory sd = it.next();
                boolean isAccessible = true;
                try { // create directories if don't exist yet
                    if (!sd.getRoot().mkdirs()) {
                        // do nothing, directory is already created
                    }
                } catch (SecurityException se) {
                    isAccessible = false;
                }
                if (!isAccessible)
                    throw new InconsistentFSStateException(sd.getRoot(),
                            "cannot access checkpoint directory.");
                StorageState curState;
                try {
                    curState = sd.analyzeStorage(HdfsConstants.StartupOption.REGULAR);
                    // sd is locked but not opened
                    switch (curState) {
                        case NON_EXISTENT:
                            // fail if any of the configured checkpoint dirs are inaccessible
                            throw new InconsistentFSStateException(sd.getRoot(),
                                    "checkpoint directory does not exist or is not accessible.");
                        case NOT_FORMATTED:
                            break;  // it's ok since initially there is no current and VERSION
                        case NORMAL:
                            break;
                        default:  // recovery is possible
                            sd.doRecover(curState);
                    }
                } catch (IOException ioe) {
                    sd.unlock();
                    throw ioe;
                }
            }
        }

        /**
         * Prepare directories for a new checkpoint.
         * <p>
         * Rename <code>current</code> to <code>lastcheckpoint.tmp</code>
         * and recreate <code>current</code>.
         *
         * @throws IOException
         */
        void startCheckpoint() throws IOException {
            for (StorageDirectory sd : storageDirs) {
                moveCurrent(sd);
            }
        }

        void endCheckpoint() throws IOException {
            for (StorageDirectory sd : storageDirs) {
                moveLastCheckpoint(sd);
            }
        }

        /**
         * Merge image and edits, and verify consistency with the signature.
         * 第二名字节点需要在内存中保存命名空间镜像的全部内容，所以他对内存的要求和NameNOde一样高。
         */
        private void doMerge(CheckpointSignature sig) throws IOException {
            getEditLog().open();
            StorageDirectory sdName = null;
            StorageDirectory sdEdits = null;
            Iterator<StorageDirectory> it;
            it = dirIterator(NameNodeDirType.IMAGE);
            if (it.hasNext())
                sdName = it.next();
            it = dirIterator(NameNodeDirType.EDITS);
            if (it.hasNext())
                sdEdits = it.next();
            if ((sdName == null) || (sdEdits == null))
                throw new IOException("Could not locate checkpoint directories");
            loadFSImage(FSImage.getImageFile(sdName, NameNodeFile.IMAGE));
            loadFSEdits(sdEdits);
            sig.validateStorageInfo(this);
            /*持久化内存元数据到磁盘中，创建命名空间镜像，即检查点。*/
            saveNamespace(false);
        }
    }
}
