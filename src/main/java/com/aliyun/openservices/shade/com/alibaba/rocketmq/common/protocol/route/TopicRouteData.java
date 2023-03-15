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

/**
 * $Id: TopicRouteData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.OrderTopicMeta;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.OrderTopicType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicRouteData extends RemotingSerializable {
    private String orderTopicConf;
    private List<QueueData> queueDatas;
    private List<BrokerData> brokerDatas;
    private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    private OrderTopicMeta meta;

    public TopicRouteData() {
        queueDatas = new ArrayList<QueueData>();
        brokerDatas = new ArrayList<BrokerData>();
        filterServerTable = new HashMap<String, List<String>>();
    }

    public TopicRouteData cloneTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setOrderTopicConf(this.orderTopicConf);

        if (this.meta != null) {
            topicRouteData.setMeta(new OrderTopicMeta(this.meta));
        }

        topicRouteData.getQueueDatas().addAll(this.queueDatas);
        topicRouteData.getBrokerDatas().addAll(this.brokerDatas);
        topicRouteData.getFilterServerTable().putAll(this.filterServerTable);

        return topicRouteData;
    }

    public TopicRouteData deepCloneTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setOrderTopicConf(this.orderTopicConf);

        if (this.meta != null) {
            topicRouteData.setMeta(new OrderTopicMeta(this.meta));
        }

        for (final QueueData queueData : this.queueDatas) {
            topicRouteData.getQueueDatas().add(new QueueData(queueData));
        }

        for (final BrokerData brokerData : this.brokerDatas) {
            topicRouteData.getBrokerDatas().add(new BrokerData(brokerData));
        }

        for (final Map.Entry<String, List<String>> listEntry : this.filterServerTable.entrySet()) {
            topicRouteData.getFilterServerTable().put(listEntry.getKey(),
                    new ArrayList<String>(listEntry.getValue()));
        }

        return topicRouteData;
    }

    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }

    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }

    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }

    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
    }

    public HashMap<String, List<String>> getFilterServerTable() {
        return filterServerTable;
    }

    public void setFilterServerTable(HashMap<String, List<String>> filterServerTable) {
        this.filterServerTable = filterServerTable;
    }

    public String getOrderTopicConf() {
        return orderTopicConf;
    }

    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }

    public OrderTopicMeta getMeta() {
        return meta;
    }

    public void setMeta(OrderTopicMeta meta) {
        this.meta = meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicRouteData data = (TopicRouteData) o;
        return Objects.equal(orderTopicConf, data.orderTopicConf) &&
                Objects.equal(queueDatas, data.queueDatas) &&
                Objects.equal(brokerDatas, data.brokerDatas) &&
                Objects.equal(filterServerTable, data.filterServerTable) &&
                Objects.equal(meta, data.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(orderTopicConf, queueDatas, brokerDatas, filterServerTable, meta);
    }

    @Override
    public String toString() {
        return "TopicRouteData{" +
                "orderTopicConf='" + orderTopicConf + '\'' +
                ", queueDatas=" + queueDatas +
                ", brokerDatas=" + brokerDatas +
                ", filterServerTable=" + filterServerTable +
                ", meta=" + meta +
                '}';
    }

    @JsonIgnore
    public int getOrderTopicQueueGroupNums() {
        if (meta == null) {
            return 0;
        }
        return meta.getQueueGroupNums();
    }

    @JsonIgnore
    public int getOrderTopicQueueGroupSize() {
        if (meta == null) {
            return 0;
        }
        return meta.getQueueGroupSize();
    }

    @JsonIgnore
    public boolean isUpgraded() {
        if (meta == null) {
            return false;
        }
        return meta.isUpgraded();
    }

    @JsonIgnore
    public int getWriteQueueGroupNums() {
        if (meta == null) {
            return 0;
        }
        return meta.getWriteQueueGroupNums();
    }

    @JsonIgnore
    public int getReadQueueGroupNums() {
        if (meta == null) {
            return 0;
        }
        return meta.getReadQueueGroupNums();
    }

    @JsonIgnore
    public OrderTopicType getOrderTopicType() {
        if (meta == null) {
            return OrderTopicType.Unknown;
        }
        return meta.getType();
    }

    @JsonIgnore
    public boolean isHAOrderTopic() {
        if (meta == null) {
            return false;
        }
        return meta.isHAOrderTopic();
    }
}
