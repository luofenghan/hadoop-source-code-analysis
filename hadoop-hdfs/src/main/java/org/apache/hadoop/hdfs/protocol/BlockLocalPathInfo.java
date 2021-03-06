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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A block and the full path information to the block data file and
 * the metadata file stored on the local file system.
 * 用于HDFS读文件的数据节点本地读优化。
 * 当客户端发现要读取的文件正好在同一台主机上时，可以不通过数据节点读数据块，而是直接读取本地文件。
 * 客户端执行本地读优化时，通过ClientDatanodeProtocol获取数据块对应的本地文件，数据节点通过BlockLocalPathInfo返回本地文件的路径。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockLocalPathInfo implements Writable {

    static {                                      // register a ctor
        WritableFactories.setFactory(BlockLocalPathInfo.class, BlockLocalPathInfo::new);
    }

    private Block block;
    private String localBlockPath = "";  // local file storing the data
    private String localMetaPath = "";   // local file storing the checksum

    public BlockLocalPathInfo() {
    }

    /**
     * Constructs BlockLocalPathInfo.
     *
     * @param b        The block corresponding to this lock path info.
     * @param file     Block data file.
     * @param metafile Metadata file for the block.
     */
    public BlockLocalPathInfo(Block b, String file, String metafile) {
        block = b;
        localBlockPath = file;
        localMetaPath = metafile;
    }

    /**
     * Get the Block data file.
     *
     * @return Block data file.
     */
    public String getBlockPath() {
        return localBlockPath;
    }

    /**
     * Get the Block metadata file.
     *
     * @return Block metadata file.
     */
    public String getMetaPath() {
        return localMetaPath;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        block.write(out);
        Text.writeString(out, localBlockPath);
        Text.writeString(out, localMetaPath);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        block = new Block();
        block.readFields(in);
        localBlockPath = Text.readString(in);
        localMetaPath = Text.readString(in);
    }

    /**
     * Get number of bytes in the block.
     *
     * @return Number of bytes in the block.
     */
    public long getNumBytes() {
        return block.getNumBytes();
    }
}
