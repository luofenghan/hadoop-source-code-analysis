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

package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * The response sent by the {@link JobTracker} to the hearbeat sent
 * periodically by the {@link TaskTracker}
 */
class HeartbeatResponse implements Writable, Configurable {
    Configuration conf = null;
    short responseId;/*心跳响应编号*/
    int heartbeatInterval;/*下次心跳的发送间隔*/
    TaskTrackerAction[] actions;/*来自JobTracker的命令，可能包括杀死作业、杀死任务、提交任务、运行任务等*/

    /*作业集合，是上次关闭JobTracker时正在运行的作业集合，重启JobTracker后需要恢复这些作业
     * 的运行状态（前提是启用了作业恢复功能），而TaskTracker收到作业集合后，需要充值这些作业对应的Reduce Task的 FetchStatus信息
     * 从而迫使这些Reduce Task重新从Map Task 端拷贝数据
     */
    Set<JobID> recoveredJobs = new HashSet<JobID>();/*恢复完成的作业列表*/

    HeartbeatResponse(short responseId, TaskTrackerAction[] actions) {
        this.responseId = responseId;
        this.actions = actions;
        this.heartbeatInterval = MRConstants.HEARTBEAT_INTERVAL_MIN;
    }

    public void setResponseId(short responseId) {
        this.responseId = responseId;
    }

    public short getResponseId() {
        return responseId;
    }

    public void setRecoveredJobs(Set<JobID> ids) {
        recoveredJobs = ids;
    }

    public Set<JobID> getRecoveredJobs() {
        return recoveredJobs;
    }

    public void setActions(TaskTrackerAction[] actions) {
        this.actions = actions;
    }

    public TaskTrackerAction[] getActions() {
        return actions;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setHeartbeatInterval(int interval) {
        this.heartbeatInterval = interval;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(responseId);
        out.writeInt(heartbeatInterval);
        if (actions == null) {
            WritableUtils.writeVInt(out, 0);
        } else {
            WritableUtils.writeVInt(out, actions.length);
            for (TaskTrackerAction action : actions) {
                WritableUtils.writeEnum(out, action.getActionId());
                action.write(out);
            }
        }
        // Write the job ids of the jobs that were recovered
        out.writeInt(recoveredJobs.size());
        for (JobID id : recoveredJobs) {
            id.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.responseId = in.readShort();
        this.heartbeatInterval = in.readInt();
        int length = WritableUtils.readVInt(in);
        if (length > 0) {
            actions = new TaskTrackerAction[length];
            for (int i = 0; i < length; ++i) {
                TaskTrackerAction.ActionType actionType =
                        WritableUtils.readEnum(in, TaskTrackerAction.ActionType.class);
                actions[i] = TaskTrackerAction.createAction(actionType);
                actions[i].readFields(in);
            }
        } else {
            actions = null;
        }
        // Read the job ids of the jobs that were recovered
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            JobID id = new JobID();
            id.readFields(in);
            recoveredJobs.add(id);
        }
    }
}
