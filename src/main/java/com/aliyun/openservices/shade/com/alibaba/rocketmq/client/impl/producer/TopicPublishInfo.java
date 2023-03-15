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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.common.ThreadLocalIndex;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.google.common.base.Preconditions;

public class TopicPublishInfo {
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private ConcurrentMap<Integer, ThreadLocalIndex> sendQueueMap = new ConcurrentHashMap<Integer, ThreadLocalIndex>();
    private TopicRouteData topicRouteData;
    /**
     * <QueueGroupId, List<MessageQueue>>
     */
    private Map<Integer, List<MessageQueue>> queueListMap = new HashMap<Integer, List<MessageQueue>>();
    private boolean mainQueuePreferred = false;

    public boolean isMainQueuePreferred() {
        return mainQueuePreferred;
    }

    public void setMainQueuePreferred(boolean mainQueuePreferred) {
        this.mainQueuePreferred = mainQueuePreferred;
    }

    public interface QueueFilter {
        boolean filter(MessageQueue mq);
    }

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    public MessageQueue selectOneMessageQueue(QueueFilter ...filter) {
        return selectOneMessageQueue(this.messageQueueList, this.sendWhichQueue, filter);
    }

    public MessageQueue selectOneMessageQueue(int queueGroupId, QueueFilter ...filter) {
        if (!this.sendQueueMap.containsKey(queueGroupId)) {
            this.sendQueueMap.putIfAbsent(queueGroupId, new ThreadLocalIndex());
        }
        return selectOneMessageQueue(this.queueListMap.get(queueGroupId), this.sendQueueMap.get(queueGroupId), filter);
    }

    private MessageQueue selectOneMessageQueue(List<MessageQueue> messageQueueList, ThreadLocalIndex sendQueue, QueueFilter ...filter) {
        if (messageQueueList == null || messageQueueList.isEmpty()) {
            return null;
        }

        if (filter != null && filter.length != 0) {
            for (int i = 0; i < messageQueueList.size(); i++) {
                int index = Math.abs(sendQueue.getAndIncrement() % messageQueueList.size());
                MessageQueue mq = messageQueueList.get(index);
                boolean filterResult = true;
                for (QueueFilter f: filter) {
                    Preconditions.checkNotNull(f);
                    filterResult &= f.filter(mq);
                }
                if (filterResult) {
                    return mq;
                }
            }

            return null;
        }

        int index = Math.abs(sendQueue.getAndIncrement() % messageQueueList.size());
        return messageQueueList.get(index);
    }

    public void resetIndex() {
        this.sendWhichQueue.reset();
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    public boolean isHAOrderTopic() {
        if (topicRouteData == null) {
            return false;
        }
        return topicRouteData.isHAOrderTopic();
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }

    public Map<Integer, List<MessageQueue>> getQueueListMap() {
        return queueListMap;
    }

    public void setQueueListMap(Map<Integer, List<MessageQueue>> queueListMap) {
        this.queueListMap = queueListMap;
    }
}
