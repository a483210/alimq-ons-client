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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.ordermessage;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.latency.MQFaultStrategy;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * The handler that handles order message processing and sending logic. This class is not thread safe. So constructing a
 * new object when it's needed.
 */
public class OrderMessageHandler {
    private MQClientInstance client;
    private String topic;
    private TopicPublishInfo info;
    private MQFaultStrategy mqFaultStrategy;
    private final InternalLogger log = ClientLogger.getLog();

    public OrderMessageHandler(MQClientInstance client, String topic, TopicPublishInfo info,
        MQFaultStrategy mqFaultStrategy) {
        this.client = client;
        this.topic = topic;
        // TopicPublishInfo should be ensured not null when constructing.
        Preconditions.checkNotNull(info);
        this.info = info;
        this.mqFaultStrategy = mqFaultStrategy;
    }

    /**
     * Use MQFaultStrategy to select available or reachable message queue as possible.
     *
     * @param mq The placeholder message queue to indicate queue group id.
     * @param lastBrokerName The previous broker where messages are sent.
     * @return Return selected message queue if the topic is HA order version. Or return argument {@code mq} if
     * targetList is empty or the topic is order version.
     */
    public MessageQueue selectOneMessageQueue(MessageQueue mq, String lastBrokerName) {
        boolean remoteFaultTolerance = this.client.getRemoteClientConfig().getRemoteFaultTolerance();
        MessageQueue targetMQ = this.mqFaultStrategy.selectOneMessageQueue(info, mq.getQueueGroupId(), lastBrokerName, remoteFaultTolerance);
        if (targetMQ == null) {
            targetMQ = mq;
        }
        return targetMQ;
    }

    /**
     * Generate message queue list of specific queue group and unpack namespace.
     *
     * @return The publish message queue list.
     */
    public List<MessageQueue> generatePublishMessageQueueList() {
        List<MessageQueue> messageQueueList = generateMessageQueueList();
        return this.client.getMQAdminImpl().parsePublishMessageQueues(messageQueueList);
    }

    /**
     * Write message queues' offset to message properties for a queue group.
     *
     * @param topic The target topic.
     * @param queueGroupId The target queue group id.
     * @param msg The message where handler writes offsets.
     */
    public void writeMQOffsetToMessage(String topic, int queueGroupId, Message msg) {
        // write offset
        List<MessageQueue> queueList = info.getQueueListMap().get(queueGroupId);
        ConcurrentMap<String, Long> offsetTable = this.client.findQueueOffsetTable(topic);
        List<String> queueGroupSnapshot = new ArrayList<String>();

        if (offsetTable != null) {
            for (MessageQueue mq : queueList) {
                // Set default value -1 when route information is lost.
                long offset = -1;

                Long newOffset = offsetTable.get(mq.generateKey());
                if (newOffset != null) {
                    offset = newOffset;
                }
                MessageAccessor.putHAOrderMessageQueueOffset(msg, mq.generateKey(), String.valueOf(offset));
                // Broker name will not repeat for in a queue group.
                queueGroupSnapshot.add(mq.generateKey());
            }
            MessageAccessor.putQueueGroupSnapshot(msg, Joiner.on(";").join(queueGroupSnapshot));
        }
    }

    /**
     * Update offset table and queue list map for this topicPublishInfo.
     */
    public void updateFromTopicPublishInfoChange() {
        try {
            // Update offset table for this topic.
            ConcurrentMap<String, Long> offsetTable = this.client.findQueueOffsetTable(topic);
            if (offsetTable == null) {
                this.client.fetchConsumeQueueOffsetFromBroker(info);
            }
        } catch (Exception e) {
            log.error("OrderMessageHandler updateFromTopicPublishInfoChange exception", e);
        }
    }

    /**
     * Process the send result.
     *
     * @param result The send result.
     * @param mq The sending message queue.
     * @return Whether retry send message.
     */
    public boolean processSendResult(SendResult result, CommunicationMode mode, MessageQueue mq) {
        boolean isRetry = false;
        switch (mode) {
            case ASYNC:
            case ONEWAY:
                return false;
            case SYNC:
                if (result.getSendStatus() != SendStatus.SEND_OK) {
                    if (this.client.getDefaultMQProducer().isRetryAnotherBrokerWhenNotStoreOK()) {
                        isRetry = true;
                    }
                } else {
                    long newOffset = result.getQueueOffset() + 1;
                    this.client.putQueueOffset(mq.getTopic(), mq.generateKey(), newOffset);
                }
            default:
                break;
        }

        return isRetry;
    }

    public static Map<Integer, List<MessageQueue>> generateQueueListMap(TopicPublishInfo info) {
        List<MessageQueue> messageQueueList = info.getMessageQueueList();
        int queueGroupNums = info.getTopicRouteData().getOrderTopicQueueGroupNums();
        // <QueueGroupId, List<MessageQueue>>
        Map<Integer, List<MessageQueue>> queueListMap = new HashMap<Integer, List<MessageQueue>>();
        for (int i = 0; i < queueGroupNums; i++) {
            // Used as a placeholder when all the queues sharing the same queueGroupId don't have route data.
            queueListMap.put(i, new ArrayList<MessageQueue>());
        }

        // To ensure that main queue is before other queue in messageQueueList.
        // Add main queue to queueList
        for (MessageQueue queue : messageQueueList) {
            List<MessageQueue> queueList = queueListMap.get(queue.getQueueGroupId());
            if (queue.isMainQueue()) {
                queueList.add(new MessageQueue(queue.getTopic(), queue.getBrokerName(), queue.getQueueId(), queue.getQueueGroupId(), true));
            }
        }

        // Add non-main queue to queueList
        for (MessageQueue queue : messageQueueList) {
            List<MessageQueue> queueList = queueListMap.get(queue.getQueueGroupId());
            if (!queue.isMainQueue()) {
                queueList.add(new MessageQueue(queue.getTopic(), queue.getBrokerName(), queue.getQueueId(), queue.getQueueGroupId(), false));
            }
        }

        return queueListMap;
    }

    List<MessageQueue> generateMessageQueueList() {
        List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
        for (int i = 0; i < info.getTopicRouteData().getWriteQueueGroupNums(); i++) {
            // Add placeholder queue to queueList for compatibility.
            messageQueueList.add(new MessageQueue(topic, "", MessageQueue.PLACEHOLDER_QUEUE_ID, i, false));
        }
        return messageQueueList;
    }
}
