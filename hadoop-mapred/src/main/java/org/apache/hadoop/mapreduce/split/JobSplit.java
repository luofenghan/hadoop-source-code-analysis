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
package org.apache.hadoop.mapreduce.split;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * This class groups the fundamental classes associated with
 * reading/writing splits. The split information is divided into
 * two parts based on the consumer of the information. The two
 * parts are the split meta information, and the raw split
 * information. The first part is consumed by the JobTracker to
 * create the tasks' locality data structures. The second part is
 * used by the maps at runtime to know what to do!
 * These pieces of information are written to two separate files.
 * The metainformation file is slurped by the JobTracker during
 * job initialization. A map task gets the meta information during
 * the launch and it reads the raw split bytes directly from the
 * file.
 */
public class JobSplit {
    static final int META_SPLIT_VERSION = 1;
    static final byte[] META_SPLIT_FILE_HEADER;

    static {
        try {
            META_SPLIT_FILE_HEADER = "META-SPL".getBytes("UTF-8");
        } catch (UnsupportedEncodingException u) {
            throw new RuntimeException(u);
        }
    }

    public static final TaskSplitMetaInfo EMPTY_TASK_SPLIT = new TaskSplitMetaInfo();

    /**
     * This represents the meta information about the task split.
     * The main fields are
     * - start offset in actual split
     * - data length that will be processed in this split
     * - hosts on which this split is local
     * 所有InputSplit对应的SplitMetaInfo将被保存到文件job.splitmetainfo中
     */
    public static class SplitMetaInfo implements Writable {
        private String[] locations;/*该InputSplit所在的hosts列表*/
        private long startOffset;/*该InputSplit元信息在job.split文件中的偏移量*/
        private long inputDataLength;/*该InputSplit的数据长度*/

        public SplitMetaInfo() {

        }

        public SplitMetaInfo(String[] locations, long startOffset, long inputDataLength) {
            this.locations = locations;
            this.startOffset = startOffset;
            this.inputDataLength = inputDataLength;
        }

        public SplitMetaInfo(InputSplit split, long startOffset) throws IOException {
            try {
                this.locations = split.getLocations();
                this.inputDataLength = split.getLength();
                this.startOffset = startOffset;
            } catch (InterruptedException ie) {
                throw new IOException(ie);
            }
        }

        public String[] getLocations() {
            return locations;
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getInputDataLength() {
            return inputDataLength;
        }

        public void setInputDataLocations(String[] locations) {
            this.locations = locations;
        }

        public void setInputDataLength(long length) {
            this.inputDataLength = length;
        }

        public void readFields(DataInput in) throws IOException {
            int len = WritableUtils.readVInt(in);
            locations = new String[len];
            for (int i = 0; i < locations.length; i++) {
                locations[i] = Text.readString(in);
            }
            startOffset = WritableUtils.readVLong(in);
            inputDataLength = WritableUtils.readVLong(in);
        }

        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, locations.length);
            for (String location : locations) {
                Text.writeString(out, location);
            }
            WritableUtils.writeVLong(out, startOffset);
            WritableUtils.writeVLong(out, inputDataLength);
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("data-size : ").append(inputDataLength).append("\n");
            buf.append("start-offset : ").append(startOffset).append("\n");
            buf.append("locations : ").append("\n");
            for (String loc : locations) {
                buf.append("  ").append(loc).append("\n");
            }
            return buf.toString();
        }
    }

    /**
     * This represents the meta information about the task split that the
     * JobTracker creates
     * 代表了JobTracker创建的任务分片的元信息
     */
    public static class TaskSplitMetaInfo {
        private TaskSplitIndex splitIndex;/*Split元信息在job.split文件中的位置*/
        private long inputDataLength;/*InputSplit的数据长度*/
        private String[] locations;/*InputSplit所在的hosts列表*/

        public TaskSplitMetaInfo() {
            this.splitIndex = new TaskSplitIndex();
            this.locations = new String[0];
        }

        public TaskSplitMetaInfo(TaskSplitIndex splitIndex, String[] locations, long inputDataLength) {
            this.splitIndex = splitIndex;
            this.locations = locations;
            this.inputDataLength = inputDataLength;
        }

        public TaskSplitMetaInfo(InputSplit split, long startOffset)
                throws InterruptedException, IOException {
            this(new TaskSplitIndex("", startOffset), split.getLocations(), split.getLength());
        }

        public TaskSplitMetaInfo(String[] locations, long startOffset, long inputDataLength) {
            this(new TaskSplitIndex("", startOffset), locations, inputDataLength);
        }

        public TaskSplitIndex getSplitIndex() {
            return splitIndex;
        }

        public String getSplitLocation() {
            return splitIndex.getSplitLocation();
        }

        public long getInputDataLength() {
            return inputDataLength;
        }

        public String[] getLocations() {
            return locations;
        }

        public long getStartOffset() {
            return splitIndex.getStartOffset();
        }
    }

    /**
     * This represents the meta information about the task split that the
     * task gets
     * JobTracker向TaskTracker分配新任务时，TaskSplitIndex用于指定
     * 新任务【待处理数据位置信息】在文件job.split中的索引
     */
    public static class TaskSplitIndex {
        private String splitLocation;/*job.split文件的位置（目录）*/
        private long startOffset;/*InputSplit信息在job.split文件中的位置*/

        public TaskSplitIndex() {
            this("", 0);
        }

        public TaskSplitIndex(String splitLocation, long startOffset) {
            this.splitLocation = splitLocation;
            this.startOffset = startOffset;
        }

        public long getStartOffset() {
            return startOffset;
        }

        public String getSplitLocation() {
            return splitLocation;
        }

        public void readFields(DataInput in) throws IOException {
            splitLocation = Text.readString(in);
            startOffset = WritableUtils.readVLong(in);
        }

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, splitLocation);
            WritableUtils.writeVLong(out, startOffset);
        }
    }
}
