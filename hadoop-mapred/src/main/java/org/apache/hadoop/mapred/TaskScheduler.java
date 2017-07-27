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
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Used by a {@link JobTracker} to schedule {@link Task}s on
 * {@link TaskTracker}s.
 * <p>
 * {@link TaskScheduler}s typically use one or more
 * {@link JobInProgressListener}s to receive notifications about jobs.
 * <p>
 * It is the responsibility of the {@link TaskScheduler}
 * to initialize tasks for a job, by calling {@link JobInProgress#initTasks()}
 * between the job being added (when
 * {@link JobInProgressListener#jobAdded(JobInProgress)} is called)
 * and tasks for that job being assigned (by
 * {@link #assignTasks(TaskTracker)}).
 *
 * @see EagerTaskInitializationListener
 */
abstract class TaskScheduler implements Configurable {

    protected Configuration conf;
    protected TaskTrackerManager taskTrackerManager;

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public synchronized void setTaskTrackerManager(TaskTrackerManager taskTrackerManager) {
        this.taskTrackerManager = taskTrackerManager;
    }

    /**
     * Lifecycle method to allow the scheduler to start any work in separate
     * threads.
     *
     * @throws IOException
     */
    public void start() throws IOException {
        // do nothing
    }

    /**
     * Lifecycle method to allow the scheduler to stop any work it is doing.
     *
     * @throws IOException
     */
    public void terminate() throws IOException {
        // do nothing
    }

    /**
     * 给指定的TaskTracker 分配任务
     *
     * @param taskTracker 寻找任务的TaskTracker
     * @return 返回给TaskTracker运行的任务列表，可能为空
     */
    public abstract List<Task> assignTasks(TaskTracker taskTracker) throws IOException;

    /**
     * Returns a collection of jobs in an order which is specific to
     * the particular scheduler.
     *
     * @param queueName
     * @return
     */
    public abstract Collection<JobInProgress> getJobs(String queueName);

    /**
     * Refresh the configuration of the scheduler.
     */
    public void refresh() throws IOException {
    }

}
