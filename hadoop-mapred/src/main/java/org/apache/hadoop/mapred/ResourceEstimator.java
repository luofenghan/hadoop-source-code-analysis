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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class responsible for modeling the resource consumption of running tasks.
 * <p>
 * For now, we just do temp space for maps
 * <p>
 * There is one ResourceEstimator per JobInProgress
 */
class ResourceEstimator {

    //Log with JobInProgress
    private static final Log LOG = LogFactory.getLog(
            "org.apache.hadoop.mapred.ResourceEstimator");

    /*所有已经运行完成的Map Task的【总输入】数据量；*/
    private long completedMapsOutputSize;
    /*所有已经运行完成的Map Task的【总输出】数据量；*/
    private long completedMapsInputSize;

    private int completedMapsUpdates;
    final private JobInProgress job;
    private int threshholdToUse;

    public ResourceEstimator(JobInProgress job) {
        this.job = job;
        threshholdToUse = job.desiredMaps() / 10;
    }

    protected synchronized void updateWithCompletedTask(TaskStatus ts,
                                                        TaskInProgress tip) {

        //-1 indicates error, which we don't average in.
        if (tip.isMapTask() && ts.getOutputSize() != -1) {
            completedMapsUpdates++;

            completedMapsInputSize += (tip.getMapInputSize() + 1);
            completedMapsOutputSize += ts.getOutputSize();

            if (LOG.isDebugEnabled()) {
                LOG.debug("completedMapsUpdates:" + completedMapsUpdates + "  " +
                        "completedMapsInputSize:" + completedMapsInputSize + "  " +
                        "completedMapsOutputSize:" + completedMapsOutputSize);
            }
        }
    }

    /**
     * @return estimated length of this job's total map output
     */
    protected synchronized long getEstimatedTotalMapOutputSize() {
        if (completedMapsUpdates < threshholdToUse) {
            return 0;
        } else {
            /*输入的总数据量*/
            long inputSize = job.getInputLength() + job.desiredMaps();
            //add desiredMaps() so that randomwriter case doesn't blow up
            //the multiplication might lead to overflow, casting it with
            //double prevents it

            /*最后乘以2表示数据量会翻倍（保守估计）。估计总共会输出多少数据量*/
            long estimate = Math.round(((double) inputSize * completedMapsOutputSize * 2.0) / completedMapsInputSize);
            if (LOG.isDebugEnabled()) {
                LOG.debug("estimate total map output will be " + estimate);
            }
            return estimate;
        }
    }

    /**
     * @return estimated length of this job's average map output
     */
    long getEstimatedMapOutputSize() {
        long estimate = 0L;
        if (job.desiredMaps() > 0) {
            /*该作业Map任务总估计输出 / map任务数*/
            estimate = getEstimatedTotalMapOutputSize() / job.desiredMaps();
        }
        return estimate;
    }

    /**
     * @return estimated length of this job's average reduce input
     */
    long getEstimatedReduceInputSize() {
        if (job.desiredReduces() == 0) {//no reduce output, so no size
            return 0;
        } else {
            return getEstimatedTotalMapOutputSize() / job.desiredReduces();
            //estimate that each reduce gets an equal share of total map output
        }
    }

    /**
     * the number of maps after which reduce starts launching
     *
     * @param numMaps the number of maps after which reduce starts
     *                launching. It acts as the upper bound for the threshhold, so
     *                that we can get right estimates before we reach these number
     *                of maps.
     */
    void setThreshhold(int numMaps) {
        threshholdToUse = Math.min(threshholdToUse, numMaps);
    }
}
