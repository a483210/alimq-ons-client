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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueueGroup;
import com.google.common.base.Objects;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueueGroup {
    private String topic;
    private int queueGroupId;

    private ProcessQueueGroup processQueueGroup;
    private MessageQueueGroup messageQueueGroup;

    private Set<QueuePair> queuePairs = new HashSet<QueuePair>();

    public QueueGroup(String topic, int queueGroupId) {
        this.topic = topic;
        this.queueGroupId = queueGroupId;
        this.processQueueGroup = new ProcessQueueGroup();
        this.messageQueueGroup = new MessageQueueGroup(topic);
    }

    public boolean isNormalMsgClean() {
        List<ProcessQueue> pqList = processQueueGroup.getProcessQueueList();
        for (ProcessQueue pq : pqList) {
            if (!pq.isNormalMsgClean()) {
                return false;
            }
        }

        return true;
    }

    public boolean isAllQueueGroupEmpty() {
        List<ProcessQueue> pqList = processQueueGroup.getProcessQueueList();
        for (ProcessQueue pq : pqList) {
            boolean hasMessage = pq.hasTempMessage();
            long unmergedMessageSize = pq.unmergedMessageSize();
            if (hasMessage && unmergedMessageSize > 0) {
                return false;
            }
        }

        return true;
    }

    public Set<QueuePair> getQueuePairs() {
        return queuePairs;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueGroupId() {
        return queueGroupId;
    }

    public ProcessQueueGroup getProcessQueueGroup() {
        return processQueueGroup;
    }

    public void setProcessQueueGroup(ProcessQueueGroup processQueueGroup) {
        this.processQueueGroup = processQueueGroup;
    }

    public MessageQueueGroup getMessageQueueGroup() {
        return messageQueueGroup;
    }

    public void setMessageQueueGroup(MessageQueueGroup messageQueueGroup) {
        this.messageQueueGroup = messageQueueGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueueGroup meta = (QueueGroup) o;

        return Objects.equal(topic, meta.topic) &&
            queueGroupId == meta.queueGroupId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topic, queueGroupId);
    }
}
