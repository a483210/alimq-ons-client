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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultLitePullConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.google.common.base.Optional;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class RebalanceLitePullImpl extends RebalanceImpl {

    private final DefaultLitePullConsumerImpl litePullConsumerImpl;

    public RebalanceLitePullImpl(DefaultLitePullConsumerImpl litePullConsumerImpl) {
        this(null, null, null, null, litePullConsumerImpl);
    }

    public RebalanceLitePullImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory, DefaultLitePullConsumerImpl litePullConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.litePullConsumerImpl = litePullConsumerImpl;
    }

    @Override public void updateQueueGroupMap(String topic, Set<MessageQueue> mqDivided) {

    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        MessageQueueListener messageQueueListener = this.litePullConsumerImpl.getDefaultLitePullConsumer().getMessageQueueListener();
        if (messageQueueListener != null) {
            try {
                messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
            } catch (Throwable e) {
                log.error("messageQueueChanged exception", e);
            }
        }
    }

    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        this.litePullConsumerImpl.getOffsetStore().persist(mq);
        this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Override
    public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
        final DefaultLitePullConsumer consumer = litePullConsumerImpl.getDefaultLitePullConsumer();
        ConsumeFromWhere consumeFromWhere = consumer.getConsumeFromWhere();
        final OffsetStore offsetStore = litePullConsumerImpl.getOffsetStore();
        final String topic = mq.getTopic();
        Optional<Long> optionalOffset;
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET: {
                try {
                    optionalOffset = offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                } catch (Throwable t) {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query consume offset from " +
                                                                              "offset store");
                }
                if (optionalOffset.isPresent()) {
                    return optionalOffset.get();
                }
                // Handle the case that no offset exists.
                // 1. for retry topic.
                if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("Failed to fetch offset from store for retry topic unexpectedly, mq={}, consumerGroup={}",
                             mq, consumerGroup);
                    return 0L;
                }
                // 1. for non-retry topic.
                try {
                    return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                } catch (MQClientException e) {
                    log.warn("Failed to compute max offset when consume from last offset, mq={}, consumerGroup={}",
                             mq, consumerGroup, e);
                    throw e;
                }
            }
            case CONSUME_FROM_FIRST_OFFSET: {
                try {
                    optionalOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                } catch (Throwable t) {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query consume offset from " +
                                                                              "offset store");
                }
                // Never distinguish retry topic, return zero if offset is absent.
                return optionalOffset.isPresent() ? optionalOffset.get() : 0L;
            }
            case CONSUME_FROM_TIMESTAMP: {
                try {
                    optionalOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                } catch (Throwable t) {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query consume offset from " +
                                                                              "offset store");
                }
                if (optionalOffset.isPresent()) {
                    return optionalOffset.get();
                }
                // Handle the case that no offset exists.
                final String consumeTimestampStr = consumer.getConsumeTimestamp();
                final Date date = UtilAll.parseDate(consumeTimestampStr, UtilAll.YYYYMMDDHHMMSS);
                if (null == date) {
                    throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Consume timestamp is illegal, please "
                                                                           + "check.");
                }
                final long consumeTimestamp = date.getTime();
                // Try to search offset by timestamp.
                try {
                    return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, consumeTimestamp);
                } catch (MQClientException e) {
                    log.warn("Failed to compute consume offset by timestamp, mq={}, consumerGroup={}, timestamp={}",
                             mq, consumerGroup, consumeTimestamp);
                    throw e;
                }
            }
            default:
                break;
        }
        throw new MQClientException(ResponseCode.ILLEGAL_OPERATION, "type of 'ConsumeFromWhere' is not supported.");
    }

    @Override
    public void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay) {
    }

    @Override public ProcessQueue createProcessQueue() {
        return new ProcessQueue();
    }

    public ProcessQueue createProcessQueue(String topicName) {
        return createProcessQueue();
    }
}
