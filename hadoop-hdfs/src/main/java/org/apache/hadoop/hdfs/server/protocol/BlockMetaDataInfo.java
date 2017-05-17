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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Meta data information for a block
 */
public class BlockMetaDataInfo extends Block {
    static final WritableFactory FACTORY = new WritableFactory() {
        public Writable newInstance() {
            return new BlockMetaDataInfo();
        }
    };

    static {                                      // register a ctor
        WritableFactories.setFactory(BlockMetaDataInfo.class, FACTORY);
    }

    private long lastScanTime;

    public BlockMetaDataInfo() {
    }

    public BlockMetaDataInfo(Block b, long lastScanTime) {
        super(b);
        this.lastScanTime = lastScanTime;
    }

    public long getLastScanTime() {
        return lastScanTime;
    }

    /** {@inheritDoc} */
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(lastScanTime);
    }

    /** {@inheritDoc} */
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        lastScanTime = in.readLong();
    }
}
