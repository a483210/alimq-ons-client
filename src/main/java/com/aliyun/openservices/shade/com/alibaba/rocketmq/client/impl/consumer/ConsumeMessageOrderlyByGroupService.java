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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.Pair;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueueGroup;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.utils.MessageUtils;

public class ConsumeMessageOrderlyByGroupService extends AbstractConsumeMessageOrderlyService {

    private final ReadWriteLock lockRequestMap = new ReentrantReadWriteLock();
    private final MergeThreadExecutor mergeExecutor;
    private final ConsumeThreadExecutor consumeExecutor;

    /**
     * <Topic, <QueueGroupId, QueueGroup>>
     */
    private Map<String, Map<Integer, QueueGroup>> queueGroupMap = new HashMap<String, Map<Integer, QueueGroup>>();

    public ConsumeMessageOrderlyByGroupService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerOrderly messageListener) {
        super(defaultMQPushConsumerImpl, messageListener);

        this.mergeExecutor = new MergeThreadExecutor(this);
        this.consumeExecutor = new ConsumeThreadExecutor(this, mergeExecutor);
    }

    @Override public void shutdown() {
        super.shutdown();
        this.consumeExecutor.shutdown();
        this.mergeExecutor.shutdown();
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            unlockAllMessageQueues();
        }
    }

    @Override public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override public void incCorePoolSize() {

    }

    @Override public void decCorePoolSize() {

    }

    @Override public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override public void allowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) {
        consumeExecutor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
    }

    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public MessageListenerOrderly getMessageListener() {
        return messageListener;
    }

    public void updateQueueGroupMap(String topic, Set<MessageQueue> mqSet) {
        try {
            lockRequestMap.writeLock().lockInterruptibly();

            try {
                Map<Integer, QueueGroup> newQueueGroupMap = new HashMap<Integer, QueueGroup>();
                List<Integer> incompleteQueueGroupIds = new ArrayList<Integer>();
                for (MessageQueue mq : mqSet) {
                    int queueGroupId = mq.getQueueGroupId();
                    if (queueGroupId == MessageQueue.NO_QUEUE_GROUP) {
                        /*
                         * can safely skip because mq without queue group id will be
                         * processed accordingly.
                         */
                        continue;
                    }

                    if (!newQueueGroupMap.containsKey(queueGroupId)) {
                        newQueueGroupMap.put(queueGroupId, new QueueGroup(topic, queueGroupId));
                    }
                    QueueGroup queueGroup = newQueueGroupMap.get(queueGroupId);
                    ProcessQueue pq = this.defaultMQPushConsumerImpl.getRebalanceImpl().processQueueTable.get(mq);
                    if (pq == null) {
                        // pq not created because mq not locked
                        incompleteQueueGroupIds.add(queueGroupId);
                    }
                    QueuePair queuePair = new QueuePair(mq, pq);
                    if (queueGroup.getQueuePairs().contains(queuePair)) {
                        continue;
                    }
                    queueGroup.getQueuePairs().add(queuePair);
                    queueGroup.getMessageQueueGroup().getMessageQueueList().add(mq);
                    queueGroup.getProcessQueueGroup().getProcessQueueList().add(pq);
                }
                for (Integer id : incompleteQueueGroupIds) {
                    newQueueGroupMap.remove(id);
                }
                if (newQueueGroupMap.isEmpty()) {
                    return;
                }
                queueGroupMap.put(topic, newQueueGroupMap);
            } finally {
                lockRequestMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            LOG.error("updateProcessQueueMap exception", e);
        }
    }

    @Override
    public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
        if (msgs == null || msgs.isEmpty()) {
            submitMergeRequest(false, new ArrayList<MessageExt>(), processQueue, messageQueue);
        } else {
            Pair<Boolean, Integer> haOrderlyMsgSize;

            do {
                haOrderlyMsgSize = getHAOrderlyMsgSize(msgs);

                // submit first part with same attribute to consume
                submitMergeRequest(haOrderlyMsgSize.getObject1(), msgs.subList(0, haOrderlyMsgSize.getObject2()), processQueue, messageQueue);

                // split second part
                msgs = msgs.subList(haOrderlyMsgSize.getObject2(), msgs.size());
            } while (!msgs.isEmpty());
        }
    }

    private Pair<Boolean, Integer> getHAOrderlyMsgSize(List<MessageExt> msgs) {
        Boolean lastMsgAttr = null;
        int size = 0;
        for (MessageExt msg : msgs) {
            boolean currentMsgAttr = isHAOrderlyMsg(msg);
            if (lastMsgAttr == null) {
                lastMsgAttr = currentMsgAttr;
            }
            /*
             * assume HA orderly msg and normal orderly msg will not appear alternately,
             * which means if current msg attr is different from last msg, then all the
             * successive msgs will all have the same attr.
             */
            if (currentMsgAttr != lastMsgAttr) {
                return new Pair<Boolean, Integer>(lastMsgAttr, size);
            } else {
                size++;
            }
        }
        return new Pair<Boolean, Integer>(lastMsgAttr, size);
    }

    private boolean isHAOrderlyMsg(MessageExt messageExt) {
        String queueGroupInfo = MessageAccessor.getQueueGroupSnapshot(messageExt);

        return queueGroupInfo != null && !queueGroupInfo.isEmpty();
    }

    public void submitMergeRequest(boolean isHAOrderlyMsg, List<MessageExt> msgs, ProcessQueue pq, final MessageQueue mq) {
        if (mq.getQueueGroupId() == MessageQueue.NO_QUEUE_GROUP) {
            if (msgs.isEmpty()) {
                return;
            }
            // is normal orderly msg and this mq has never received HA orderly msg.
            if (!isHAOrderlyMsg && !pq.isReceivedHAMsg()) {
                pq.setNormalMsgClean(false);
                pq.getMergeProgress().addAndGet(msgs.size());
                List<Pair<QueuePair, List<MessageExt>>> msgPairList = new ArrayList<Pair<QueuePair, List<MessageExt>>>();
                msgPairList.add(new Pair<QueuePair, List<MessageExt>>(new QueuePair(mq, pq), msgs));

                QueueGroup queueGroup = new QueueGroup(mq.getTopic(), MessageQueue.NO_QUEUE_GROUP);
                queueGroup.getMessageQueueGroup().getMessageQueueList().add(mq);
                queueGroup.getProcessQueueGroup().getProcessQueueList().add(pq);

                this.submitConsumeRequest(msgPairList, queueGroup);
            } else {
                if (!pq.isReceivedHAMsg()) {
                    pq.setReceivedHAMsg(true);
                    // use out of date route info, trigger update and rebalance.
                    LOG.warn("Topic {} upgrade incomplete, wait for route update and rebalance", mq.getTopic());
                }
            }
            return;
        }

        try {
            lockRequestMap.readLock().lockInterruptibly();
            try {

                if (!queueGroupMap.containsKey(mq.getTopic())) {
                    return;
                }

                QueueGroup queueGroup = queueGroupMap.get(mq.getTopic()).get(mq.getQueueGroupId());

                if (queueGroup == null) {
                    return;
                }

                MergeRequest mergeRequest = new MergeRequest(queueGroup, this);
                if (!queueGroup.isNormalMsgClean()) {
                    mergeExecutor.submitLater(mergeRequest, 1000);
                    return;
                }

                mergeExecutor.submit(mergeRequest, false);

            } finally {
                lockRequestMap.readLock().unlock();
            }

        } catch (InterruptedException e) {
            LOG.error("handleMergeRequest exception", e);
        }
    }

    public synchronized boolean lockMQGroup(MessageQueueGroup messageQueueGroup) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lockBatch(new HashSet<MessageQueue>(messageQueueGroup.getMessageQueueList()), true);
        }
        return false;
    }

    void submitConsumeRequest(List<Pair<QueuePair, List<MessageExt>>> msgPairList,
        QueueGroup queueGroup) {
        if (isConsumeAccelerated(queueGroup.getTopic())) {
            int totalSize = this.defaultMQPushConsumer.getMaxConcurrencyForOrderQueue();
            Map<Integer, List<Pair<QueuePair, Integer>>> queuePairMap = new HashMap<Integer, List<Pair<QueuePair, Integer>>>();
            for (Pair<QueuePair, List<MessageExt>> messagePair : msgPairList) {
                Set<Integer> shardingKeyIndexSet = MessageUtils.getShardingKeyIndexes(messagePair.getObject2(), totalSize);
                Map<Integer, Integer> consumeBatchSizeMap = getConsumeBatchSize(shardingKeyIndexSet, new ArrayList<MessageExt>(messagePair.getObject2()), totalSize);
                for (Integer shardingKeyIndex : shardingKeyIndexSet) {
                    if (!queuePairMap.containsKey(shardingKeyIndex)) {
                        queuePairMap.put(shardingKeyIndex, new ArrayList<Pair<QueuePair, Integer>>());
                    }
                    queuePairMap.get(shardingKeyIndex).add(new Pair<QueuePair, Integer>(messagePair.getObject1(), consumeBatchSizeMap.get(shardingKeyIndex)));
                }
            }

            for (Integer shardingKeyIndex : queuePairMap.keySet()) {
                ConsumeRequest req = new ConsumeRequest(queueGroup, consumeExecutor, this, shardingKeyIndex);
                consumeExecutor.submit(req, queuePairMap.get(shardingKeyIndex), false);
            }
        } else {
            ConsumeRequest req = new ConsumeRequest(queueGroup, consumeExecutor, this);
            List<Pair<QueuePair, Integer>> queuePairList = new ArrayList<Pair<QueuePair, Integer>>();
            for (Pair<QueuePair, List<MessageExt>> msgPair : msgPairList) {
                queuePairList.add(new Pair<QueuePair, Integer>(msgPair.getObject1(), msgPair.getObject2().size()));
            }
            consumeExecutor.submit(req, queuePairList, false);
        }
    }

    private Map<Integer, Integer> getConsumeBatchSize(Set<Integer> shardingKeyIndexSet, List<MessageExt> messageList, int totalSize) {
        Map<Integer, Integer> consumeBatchSizeMap = new HashMap<Integer, Integer>();
        for (Integer shardingKeyIndex : shardingKeyIndexSet) {
            if (!consumeBatchSizeMap.containsKey(shardingKeyIndex)) {
                consumeBatchSizeMap.put(shardingKeyIndex, 0);
            }
            for (MessageExt messageExt : messageList) {
                if (shardingKeyIndex.equals(MessageUtils.getShardingKeyIndexByMsg(messageExt, totalSize))) {
                    consumeBatchSizeMap.put(shardingKeyIndex, consumeBatchSizeMap.get(shardingKeyIndex) + 1);
                }
            }
        }
        return consumeBatchSizeMap;
    }

    public Map<String, Map<Integer, QueueGroup>> getQueueGroupMap() {
        return queueGroupMap;
    }

    public boolean processConsumeResult(
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeOrderlyContext context,
        final ConsumeRequest consumeRequest,
        final MessageQueue messageQueue,
        final ProcessQueue processQueue,
        final Pair<QueuePair, Integer> queuePair,
        final int msgsConsumed
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        int shardIndex = consumeRequest.getShardingKeyIndex();
        if (context.isAutoCommit()) {
            switch (status) {
                case COMMIT:
                case ROLLBACK:
                    LOG.warn("the message queue consume result is illegal, we think you want to ack these message {}", messageQueue);
                case SUCCESS:
                    commitOffset = processQueue.commit(msgs, shardIndex);
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, messageQueue.getTopic(), msgs.size());
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, messageQueue.getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        queuePair.setObject2(queuePair.getObject2() - msgsConsumed);
                        consumeRequest.getQueueToConsume().addFirst(queuePair);
                        processQueue.makeMessageToConsumeAgain(msgs, shardIndex);
                        consumeExecutor.submitLater(consumeRequest, context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    } else {
                        commitOffset = processQueue.commit(msgs, shardIndex);
                    }
                    break;
                default:
                    break;
            }
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, messageQueue.getTopic(), msgs.size());
                    break;
                case COMMIT:
                    commitOffset = processQueue.commit(msgs, shardIndex);
                    break;
                case ROLLBACK:
                    queuePair.setObject2(queuePair.getObject2() - msgsConsumed);
                    consumeRequest.getQueueToConsume().addFirst(queuePair);
                    processQueue.rollback(msgs, shardIndex);
                    consumeExecutor.submitLater(consumeRequest, context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, messageQueue.getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        queuePair.setObject2(queuePair.getObject2() - msgsConsumed);
                        consumeRequest.getQueueToConsume().addFirst(queuePair);
                        processQueue.makeMessageToConsumeAgain(msgs, shardIndex);
                        consumeExecutor.submitLater(consumeRequest, context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        if (commitOffset >= 0 && !processQueue.isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(messageQueue, commitOffset, false);
        }

        if (!processQueue.isNormalMsgClean()) {
            if (processQueue.getMergeProgress().get() <= processQueue.getConsumeProgress().get()) {
                processQueue.setNormalMsgClean(true);
            }
        }

        return continueConsume;
    }

    private boolean isConsumeAccelerated(String topicName) {
        return this.defaultMQPushConsumerImpl.isConsumeAccelerated(topicName);
    }
}
