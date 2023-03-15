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

import java.io.Serializable;
import java.util.List;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.QueueDesc;
import com.google.common.base.Objects;

public class QueueGroupConfig implements Serializable {
    private static final long serialVersionUID = 7255246811167590294L;

    private String topicName;

    /**
     * The queue list belong to a queue group of the topic in a broker.
     */
    private List<QueueDesc> queueDescList;

    private OrderTopicMeta meta;

    public QueueGroupConfig() {
    }

    public QueueGroupConfig(String topicName, List<QueueDesc> queueDescList, OrderTopicMeta meta) {
        this.topicName = topicName;
        this.queueDescList = queueDescList;
        this.meta = meta;
    }

    public List<QueueDesc> getQueueDescList() {
        return queueDescList;
    }

    public void setQueueDescList(List<QueueDesc> queueDescList) {
        this.queueDescList = queueDescList;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueueGroupConfig config = (QueueGroupConfig) o;
        return Objects.equal(topicName, config.topicName) &&
            Objects.equal(queueDescList, config.queueDescList) &&
            Objects.equal(meta, config.meta);
    }

    @Override public int hashCode() {
        return Objects.hashCode(topicName, queueDescList, meta);
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public boolean isHAOrderTopic() {
        return this.queueDescList != null && this.queueDescList.size() != 0;
    }

    public OrderTopicMeta getMeta() {
        return meta;
    }

    public void setMeta(OrderTopicMeta meta) {
        this.meta = meta;
    }
}
