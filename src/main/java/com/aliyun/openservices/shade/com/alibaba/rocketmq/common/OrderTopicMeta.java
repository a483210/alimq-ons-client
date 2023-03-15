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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import com.google.common.base.Objects;

import java.io.Serializable;

public class OrderTopicMeta extends RemotingSerializable implements Serializable {
    private static final long serialVersionUID = -5864962554367122826L;

    // The number of queueGroup in the topic for high availability order message.
    private int queueGroupNums;
    // The number of queues in a queueGroup.
    private int queueGroupSize;
    // To indicate whether the topic is upgraded from old order topic.
    private boolean upgraded;

    // For the convenience of scaling queue group.
    private int writeQueueGroupNums;
    private int readQueueGroupNums;

    // Indicate the type of order topic.
    OrderTopicType type = OrderTopicType.Unknown;

    private boolean haOrderTopic = true;

    public OrderTopicMeta() {
    }

    public OrderTopicMeta(OrderTopicMeta meta) {
        this.queueGroupNums = meta.queueGroupNums;
        this.queueGroupSize = meta.queueGroupSize;
        this.upgraded = meta.upgraded;

        this.writeQueueGroupNums = meta.writeQueueGroupNums;
        this.readQueueGroupNums = meta.readQueueGroupNums;
        this.type = meta.type;
        this.haOrderTopic = meta.haOrderTopic;
    }

    public OrderTopicMeta(int queueGroupNums, int queueGroupSize) {
        this(queueGroupNums, queueGroupSize, false);
    }

    public OrderTopicMeta(int queueGroupNums, int queueGroupSize, boolean upgraded) {
        this(queueGroupNums, queueGroupSize, queueGroupNums, queueGroupNums, upgraded);
    }

    public OrderTopicMeta(int queueGroupNums, int queueGroupSize, int writeQueueGroupNums, int readQueueGroupNums,
        boolean upgraded) {
        if (queueGroupNums == 1) {
            type = OrderTopicType.Global;
        } else if (queueGroupNums > 1) {
            type = OrderTopicType.Partition;
        }
        this.queueGroupNums = queueGroupNums;
        this.queueGroupSize = queueGroupSize;
        this.writeQueueGroupNums = writeQueueGroupNums;
        this.readQueueGroupNums = readQueueGroupNums;
        this.upgraded = upgraded;
    }

    public int getQueueGroupNums() {
        return queueGroupNums;
    }

    public void setQueueGroupNums(int queueGroupNums) {
        this.queueGroupNums = queueGroupNums;
    }

    public int getQueueGroupSize() {
        return queueGroupSize;
    }

    public void setQueueGroupSize(int queueGroupSize) {
        this.queueGroupSize = queueGroupSize;
    }

    public boolean isUpgraded() {
        return upgraded;
    }

    public void setUpgraded(boolean upgraded) {
        this.upgraded = upgraded;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        OrderTopicMeta meta = (OrderTopicMeta) o;
        return queueGroupNums == meta.queueGroupNums &&
            queueGroupSize == meta.queueGroupSize &&
            upgraded == meta.upgraded &&
            writeQueueGroupNums == meta.writeQueueGroupNums &&
            readQueueGroupNums == meta.readQueueGroupNums &&
            type == meta.type;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queueGroupNums, queueGroupSize, upgraded, writeQueueGroupNums, readQueueGroupNums, type);
    }

    @Override
    public String toString() {
        return "OrderTopicMeta{" +
            "queueGroupNums=" + queueGroupNums +
            ", queueGroupSize=" + queueGroupSize +
            ", upgraded=" + upgraded +
            ", writeQueueGroupNums=" + writeQueueGroupNums +
            ", readQueueGroupNums=" + readQueueGroupNums +
            ", type=" + type +
            '}';
    }

    public int getWriteQueueGroupNums() {
        return writeQueueGroupNums;
    }

    public void setWriteQueueGroupNums(int writeQueueGroupNums) {
        this.writeQueueGroupNums = writeQueueGroupNums;
    }

    public int getReadQueueGroupNums() {
        return readQueueGroupNums;
    }

    public void setReadQueueGroupNums(int readQueueGroupNums) {
        this.readQueueGroupNums = readQueueGroupNums;
    }

    public OrderTopicType getType() {
        return type;
    }

    public void setType(OrderTopicType type) {
        this.type = type;
    }

    public boolean isHAOrderTopic() {
        return haOrderTopic;
    }

    public void setHAOrderTopic(boolean haOrderTopic) {
        this.haOrderTopic = haOrderTopic;
    }
}
