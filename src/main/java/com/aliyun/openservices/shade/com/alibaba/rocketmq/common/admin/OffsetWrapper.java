/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.admin;

public class OffsetWrapper {
    private long brokerOffset;
    private long consumerOffset;

    //The store time of the latest consumed message
    private long lastTimestamp;

    /**
     * Estimated number of messages matching filter criteria of the context consumer group.
     */
    private long estimatedAccumulation = -1;

    /**
     * The store time of the earliest unconsumed message
     * <p> if has no unconsumed message, it is 0
     */
    private long earliestUnconsumedTimestamp;

    public long getBrokerOffset() {
        return brokerOffset;
    }

    public void setBrokerOffset(long brokerOffset) {
        this.brokerOffset = brokerOffset;
    }

    public long getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public long getEarliestUnconsumedTimestamp() {
        return earliestUnconsumedTimestamp;
    }

    public void setEarliestUnconsumedTimestamp(long earliestUnconsumedTimestamp) {
        this.earliestUnconsumedTimestamp = earliestUnconsumedTimestamp;
    }

    public long getEstimatedAccumulation() {
        return estimatedAccumulation;
    }

    public void setEstimatedAccumulation(long estimatedAccumulation) {
        this.estimatedAccumulation = estimatedAccumulation;
    }
}
