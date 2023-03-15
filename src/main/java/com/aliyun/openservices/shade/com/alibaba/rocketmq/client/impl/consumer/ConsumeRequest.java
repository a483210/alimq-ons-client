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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeReturnType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.Pair;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueueGroup;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.google.common.base.Objects;

public class ConsumeRequest implements Runnable {
    private InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "5000"));

    private final MessageQueueGroup messageQueueGroup;
    private final ProcessQueueGroup processQueueGroup;
    private final ConsumeThreadExecutor consumeThreadExecutor;
    private final ConsumeMessageOrderlyByGroupService cs;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final MessageListenerOrderly messageListener;
    private final String consumerGroup;

    private final MessageQueueGroupLock consumeRequestLock;
    private final MessageQueueGroupLock shardingKeyLock;

    /**
     * <QueuePair, ConsumeBatchSize>
     */
    private final LinkedBlockingDeque<Pair<QueuePair, Integer>> queueToConsume = new LinkedBlockingDeque<Pair<QueuePair, Integer>>();
    private final int shardingKeyIndex;

    public ConsumeRequest(QueueGroup queueGroup, ConsumeThreadExecutor consumeThreadExecutor, ConsumeMessageOrderlyByGroupService cs) {
        this(queueGroup, consumeThreadExecutor, cs, 0);
    }

    public ConsumeRequest(QueueGroup queueGroup, ConsumeThreadExecutor consumeThreadExecutor, ConsumeMessageOrderlyByGroupService cs, int shardingKeyIndex) {
        this.messageQueueGroup = queueGroup.getMessageQueueGroup();
        this.processQueueGroup = queueGroup.getProcessQueueGroup();
        this.consumeThreadExecutor = consumeThreadExecutor;
        this.cs = cs;
        this.defaultMQPushConsumer = cs.getDefaultMQPushConsumer();
        this.defaultMQPushConsumerImpl = cs.getDefaultMQPushConsumerImpl();
        this.messageListener = cs.getMessageListener();
        this.consumerGroup = defaultMQPushConsumer.getConsumerGroup();
        this.shardingKeyIndex = shardingKeyIndex;

        this.consumeRequestLock = consumeThreadExecutor.getConsumeRequestLock();
        this.shardingKeyLock = consumeThreadExecutor.getShardingKeyLock();
    }

    public LinkedBlockingDeque<Pair<QueuePair, Integer>> getQueueToConsume() {
        return queueToConsume;
    }

    public int getShardingKeyIndex() {
        return shardingKeyIndex;
    }

    public MessageQueueGroup getMessageQueueGroup() {
        return messageQueueGroup;
    }

    public ProcessQueueGroup getProcessQueueGroup() {
        return processQueueGroup;
    }

    @Override
    public void run() {
        try {
            // lock on sharding key index
            final Object objLock = shardingKeyLock.fetchLockObject(messageQueueGroup, shardingKeyIndex);
            final int consumeBatchMaxSize =
                ConsumeRequest.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
            synchronized (objLock) {
                boolean continueConsume = true;
                final long beginTime = System.currentTimeMillis();
                for (int invokeCnt = 0; continueConsume; invokeCnt++) {

                    long interval = System.currentTimeMillis() - beginTime;
                    if (invokeCnt > 0 && interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                        consumeThreadExecutor.submit(this, null, true);
                        break;
                    }

                    Pair<QueuePair, Integer> queuePair = queueToConsume.poll();

                    Object lock = consumeRequestLock.fetchLockObject(messageQueueGroup, this.shardingKeyIndex);
                    synchronized (lock) {
                        if (queuePair == null) {
                            queuePair = queueToConsume.poll();
                            if (queuePair == null) {
                                consumeThreadExecutor.remove(this);
                                return;
                            }
                        }
                    }

                    MessageQueue messageQueue = queuePair.getObject1().getMessageQueue();
                    ProcessQueue processQueue = queuePair.getObject1().getProcessQueue();
                    int consumeBatchSize = queuePair.getObject2();
                    int msgConsumed = 0;

                    while (msgConsumed < consumeBatchSize && continueConsume) {
                        if (processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", messageQueue);
                            this.consumeThreadExecutor.remove(this);
                            return;
                        }

                        if (MessageModel.CLUSTERING.equals(defaultMQPushConsumerImpl.messageModel())
                            && !processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", messageQueue);
                            queuePair.setObject2(consumeBatchSize - msgConsumed);
                            queueToConsume.addFirst(queuePair);
                            consumeThreadExecutor.tryLockLaterAndConsumeAgain(this, messageQueue, 10);
                            return;
                        }

                        if (MessageModel.CLUSTERING.equals(defaultMQPushConsumerImpl.messageModel())
                            && processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", messageQueue);
                            queuePair.setObject2(consumeBatchSize - msgConsumed);
                            queueToConsume.addFirst(queuePair);
                            consumeThreadExecutor.tryLockLaterAndConsumeAgain(this, messageQueue, 10);
                            return;
                        }

                        int msgsToConsume = Math.min(consumeBatchMaxSize, consumeBatchSize - msgConsumed);

                        // take messages by sharding key index
                        List<MessageExt> msgs = processQueue.takeMessagesByShardingKeyIndex(shardingKeyIndex, msgsToConsume);

                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

                        ConsumeOrderlyStatus status = null;
                        ConsumeMessageContext consumeMessageContext = null;
                        if (defaultMQPushConsumerImpl.hasHook()) {
                            consumeMessageContext = new ConsumeMessageContext();
                            consumeMessageContext
                                .setConsumerGroup(consumerGroup);
                            consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                            consumeMessageContext.setMq(messageQueue);
                            consumeMessageContext.setMsgList(msgs);
                            consumeMessageContext.setSuccess(false);
                            // init the consume context type
                            consumeMessageContext.setProps(new HashMap<String, String>());
                            defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                        }

                        boolean hasException = false;
                        long beginTimestamp = System.currentTimeMillis();
                        final ConsumeOrderlyContext context = new ConsumeOrderlyContext(messageQueue);

                        try {
                            processQueue.getLockConsume().readLock().lock();

                            try {
                                if (processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}", messageQueue);
                                    this.consumeThreadExecutor.remove(this);
                                    return;
                                }

                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                    RemotingHelper.exceptionSimpleDesc(e),
                                    consumerGroup,
                                    msgs,
                                    messageQueue);
                                hasException = true;
                            } finally {
                                processQueue.getLockConsume().readLock().unlock();
                            }
                        } catch (Throwable e) {
                            hasException = true;
                        }

                        if (null == status
                            || ConsumeOrderlyStatus.ROLLBACK == status
                            || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                            log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                consumerGroup,
                                msgs,
                                messageQueue);
                        }

                        long consumeRT = System.currentTimeMillis() - beginTimestamp;
                        ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                        if (null == status) {
                            if (hasException) {
                                returnType = ConsumeReturnType.EXCEPTION;
                            } else {
                                returnType = ConsumeReturnType.RETURNNULL;
                            }
                        } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                            returnType = ConsumeReturnType.TIME_OUT;
                        } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                            returnType = ConsumeReturnType.FAILED;
                        } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                            returnType = ConsumeReturnType.SUCCESS;
                        }

                        if (defaultMQPushConsumerImpl.hasHook()) {
                            consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                        }

                        if (null == status) {
                            status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }

                        if (defaultMQPushConsumerImpl.hasHook()) {
                            consumeMessageContext.setStatus(status.toString());
                            consumeMessageContext
                                .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                            defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                        }

                        cs.getConsumerStatsManager()
                            .incConsumeRT(consumerGroup, messageQueue.getTopic(), consumeRT);

                        continueConsume = cs.processConsumeResult(msgs, status, context, this, messageQueue, processQueue, queuePair, msgConsumed);
                        msgConsumed += msgsToConsume;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Run consume request exception", e);
        }
    }


    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumeRequest other = (ConsumeRequest) o;
        return shardingKeyIndex == other.shardingKeyIndex
            && Objects.equal(messageQueueGroup, other.messageQueueGroup)
            && Objects.equal(processQueueGroup, other.processQueueGroup);
    }

    @Override public int hashCode() {
        return Objects.hashCode(messageQueueGroup, processQueueGroup, shardingKeyIndex);
    }
}
