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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route;

import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * QueueDesc is a representation of MessageQueue in server side; it also used to represent the affiliation to a
 * QueueGroup.
 */
public class QueueDesc implements Serializable {
    public static final int NOT_SET = -1;

    private static final long serialVersionUID = -2923393070458262192L;

    private int messageQueueId;
    private int queueGroupId;

    private boolean isMainQueue;

    public QueueDesc() {
        messageQueueId = NOT_SET;
        queueGroupId = NOT_SET;
    }

    public QueueDesc(QueueDesc desc) {
        this.messageQueueId = desc.messageQueueId;
        this.queueGroupId = desc.queueGroupId;
        this.isMainQueue = desc.isMainQueue;
    }

    public QueueDesc(int queueGroupId) {
        this(NOT_SET, queueGroupId);
    }

    public QueueDesc(int messageQueueId, int queueGroupId) {
        this(messageQueueId, queueGroupId, false);
    }

    public QueueDesc(int messageQueueId, int queueGroupId, boolean isMainQueue) {
        this.messageQueueId = messageQueueId;
        this.queueGroupId = queueGroupId;
        this.isMainQueue = isMainQueue;
    }

    public boolean isMainQueue() {
        return isMainQueue;
    }

    public void setMainQueue(boolean mainQueue) {
        isMainQueue = mainQueue;
    }

    public int getMessageQueueId() {
        return messageQueueId;
    }

    public void setMessageQueueId(int messageQueueId) {
        this.messageQueueId = messageQueueId;
    }

    public int getQueueGroupId() {
        return queueGroupId;
    }

    public void setQueueGroupId(int queueGroupId) {
        this.queueGroupId = queueGroupId;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueueDesc desc = (QueueDesc) o;
        return messageQueueId == desc.messageQueueId &&
            queueGroupId == desc.queueGroupId &&
            isMainQueue == desc.isMainQueue;
    }

    @Override public int hashCode() {
        return Objects.hashCode(messageQueueId, queueGroupId, isMainQueue);
    }

    @Override public String toString() {
        return new ToStringBuilder(this)
            .append("messageQueueId", messageQueueId)
            .append("queueGroupId", queueGroupId)
            .append("isMainQueue", isMainQueue)
            .toString();
    }
}
