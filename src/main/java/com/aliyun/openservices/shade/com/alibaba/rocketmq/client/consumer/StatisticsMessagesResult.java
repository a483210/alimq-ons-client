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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

public class StatisticsMessagesResult extends RemotingSerializable {
    private long activeMessages;
    private long inactiveMessages;
    private long delayMessages;
    private long totalMessages;

    public StatisticsMessagesResult() {
        this.activeMessages = 0;
        this.inactiveMessages = 0;
        this.delayMessages = 0;
        this.totalMessages = 0;
    }

    public StatisticsMessagesResult(long activeMessages, long delayMessages, long totalMessages) {
        this.activeMessages = activeMessages;
        this.inactiveMessages = 0;
        this.delayMessages = delayMessages;
        this.totalMessages = totalMessages;
    }

    public long getActiveMessages() {
        return activeMessages;
    }

    public void setActiveMessages(long activeMessages) {
        this.activeMessages = activeMessages;
    }

    public long getInactiveMessages() {
        return inactiveMessages;
    }

    public void setInactiveMessages(long inactiveMessages) {
        this.inactiveMessages = inactiveMessages;
    }

    public long getDelayMessages() {
        return delayMessages;
    }

    public void setDelayMessages(long delayMessages) {
        this.delayMessages = delayMessages;
    }

    public long getTotalMessages() {
        return totalMessages;
    }

    public void setTotalMessages(long totalMessages) {
        this.totalMessages = totalMessages;
    }

    @Override
    public String toString() {
        return "StatisticsMessagesResult [activeMessages=" + activeMessages + ",inactiveMessages="
            + inactiveMessages + ",delayMessages=" + delayMessages + ",totalMessages=" + totalMessages + "]";
    }
}
