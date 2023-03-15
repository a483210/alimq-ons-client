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
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
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
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.google.common.base.Optional;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RebalancePushImpl extends RebalanceImpl {
    private final static long UNLOCK_DELAY_TIME_MILLS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    @Override
    public void updateQueueGroupMap(String topic, Set<MessageQueue> mqDivided) {
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()) {
            if (this.defaultMQPushConsumerImpl.getConsumeMessageService() instanceof ConsumeMessageOrderlyByGroupService) {
                ConsumeMessageOrderlyByGroupService consumeService = (ConsumeMessageOrderlyByGroupService) this.defaultMQPushConsumerImpl.getConsumeMessageService();
                consumeService.updateQueueGroupMap(topic, mqDivided);
            }
        }
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {

        /**
         * When rebalance result changed, should update subscription's version to notify broker.
         * Fix: inconsistency subscription may lead to consumer miss messages.
         */
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        long newVersion = System.currentTimeMillis();
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);

        int currentQueueCount = this.processQueueTable.size();
        if (currentQueueCount != 0) {
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            if (pullThresholdForTopic != -1) {
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }

            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
            if (pullThresholdSizeForTopic != -1) {
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        // notify broker
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithTimedLock();
    }

    @Override
    public boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq) {
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
            && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {

            // commit offset immediately
            this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);

            // remove order message queue: unlock & remove
            return tryRemoveOrderMessageQueue(mq, pq);
        } else {
            this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
            this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
            return true;
        }
    }

    private boolean tryRemoveOrderMessageQueue(final MessageQueue mq, final ProcessQueue pq) {
        try {
            // unlock & remove when no message is consuming or UNLOCK_DELAY_TIME_MILLS timeout (Backwards compatibility)
            boolean forceUnlock = pq.isDropped() && System.currentTimeMillis() > pq.getLastLockTimestamp() + UNLOCK_DELAY_TIME_MILLS;
            if (forceUnlock || pq.getLockConsume().writeLock().tryLock(500, TimeUnit.MILLISECONDS)) {
                try {
                    RebalancePushImpl.this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
                    RebalancePushImpl.this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);

                    pq.setLocked(false);
                    RebalancePushImpl.this.unlock(mq, true);
                    return true;
                } finally {
                    if (!forceUnlock) {
                        pq.getLockConsume().writeLock().unlock();
                    }
                }
            } else {
                pq.incTryUnlockTimes();
            }
        } catch (Exception e) {
            pq.incTryUnlockTimes();
        }

        return false;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Override
    public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
        final DefaultMQPushConsumer consumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        final ConsumeFromWhere consumeFromWhere = consumer.getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        final String topic = mq.getTopic();
        Optional<Long> optionalOffset;
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
            case CONSUME_FROM_LAST_OFFSET: {
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
                // 1. for retry topic.
                if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("Failed to fetch offset from store for retry topic unexpectedly, mq={}, consumerGroup={}",
                             mq, consumerGroup);
                    return 0L;
                }
                // 2. for non-retry topic.
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
                final String consumeTimestampStr =
                        consumer.getConsumeTimestamp();
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
        for (PullRequest pullRequest : pullRequestList) {
            if (delay <= 0) {
                this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            } else {
                this.defaultMQPushConsumerImpl.executePullRequestLater(pullRequest, delay);
            }
        }
    }

    @Override
    public ProcessQueue createProcessQueue() {
        return new ProcessQueue();
    }

    @Override public ProcessQueue createProcessQueue(String topicName) {
        DefaultMQPushConsumer consumer = defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        MessageListener listener = consumer.getMessageListener();
        if (listener instanceof MessageListenerOrderly && defaultMQPushConsumerImpl.isConsumeAccelerated(topicName)) {
            return new ProcessQueue(consumer.getMaxConcurrencyForOrderQueue());
        }
        return createProcessQueue();
    }
}
