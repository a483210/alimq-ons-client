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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeReturnType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.utils.MessageUtils;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumeMessageOrderlyService extends AbstractConsumeMessageOrderlyService {
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "5000"));
    
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ConcurrentSet<ConsumeRequest> consumeRequestSet = new ConcurrentSet<ConsumeRequest>();
    private final ThreadPoolExecutor consumeExecutor;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final MessageQueueLock consumeRequestLock = new MessageQueueLock();

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerOrderly messageListener) {
        super(defaultMQPushConsumerImpl, messageListener);
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
            defaultMQPushConsumer.getConsumeThreadMin(),
            defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.consumeExecutor.shutdown();
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMessageQueues();
        }
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public void allowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) {
        consumeExecutor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
    }

    @Override
    public void submitConsumeRequest(final List<MessageExt> msgs,
                                     final ProcessQueue processQueue,
                                     final MessageQueue messageQueue) {
        if (msgs == null || msgs.isEmpty()) {
            return;
        }

        if (isConsumeAccelerated(messageQueue.getTopic())) {
            int totalSize = defaultMQPushConsumer.getMaxConcurrencyForOrderQueue();
            Set<Integer> shardingKeyIndexSet = MessageUtils.getShardingKeyIndexes(msgs, totalSize);
            for (Integer shardingKeyIndex : shardingKeyIndexSet) {
                ConsumeRequest req = new ConsumeRequest(processQueue, messageQueue, shardingKeyIndex);
                submitConsumeRequest(req, false);
            }
        } else {
            ConsumeRequest req = new ConsumeRequest(processQueue, messageQueue);
            submitConsumeRequest(req, false);
        }
    }

    public void tryLockLaterAndReconsume(final ConsumeRequest consumeRequest, final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMessageQueue(consumeRequest.getMessageQueue());
                if (lockOK) {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(consumeRequest, 10);
                } else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(consumeRequest, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    private void removeConsumeRequest(final ConsumeRequest consumeRequest) {
        consumeRequestSet.remove(consumeRequest);
    }

    private void submitConsumeRequest(final ConsumeRequest consumeRequest, boolean force) {
        Object lock = consumeRequestLock.fetchLockObject(consumeRequest.getMessageQueue(), consumeRequest.shardingKeyIndex);
        synchronized (lock) {
            boolean isNewReq = consumeRequestSet.add(consumeRequest);
            if (force || isNewReq) {
                try {
                    consumeExecutor.submit(consumeRequest);
                } catch (Exception e) {
                    LOG.error("error submit consume request: {}, mq: {}, shardingKeyIndex: {}",
                        e.toString(), consumeRequest.getMessageQueue(), consumeRequest.getShardingKeyIndex());
                }
            }
        }
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest, final long suspendTimeMillis) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                submitConsumeRequest(consumeRequest, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    public boolean processConsumeResult(
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeOrderlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        int shardIndex = consumeRequest.getShardingKeyIndex();
        if (context.isAutoCommit()) {
            switch (status) {
                case COMMIT:
                case ROLLBACK:
                    LOG.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                        consumeRequest.getMessageQueue());
                case SUCCESS:
                    commitOffset = consumeRequest.getProcessQueue().commit(msgs, shardIndex);
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs, shardIndex);
                        this.submitConsumeRequestLater(consumeRequest, context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    } else {
                        commitOffset = consumeRequest.getProcessQueue().commit(msgs, shardIndex);
                    }
                    break;
                default:
                    break;
            }
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commit(msgs, shardIndex);
                    break;
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback(msgs, shardIndex);
                    this.submitConsumeRequestLater(consumeRequest, context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs, shardIndex);
                        this.submitConsumeRequestLater(consumeRequest, context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    class ConsumeRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;
        private int shardingKeyIndex = 0;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
            this.shardingKeyIndex = 0;
        }

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue, int shardingKeyIndex) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
            this.shardingKeyIndex = shardingKeyIndex;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public int getShardingKeyIndex() {
            return shardingKeyIndex;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                LOG.warn("run, message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                ConsumeMessageOrderlyService.this.removeConsumeRequest(this);
                return;
            }

            // lock on sharding key index
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue, shardingKeyIndex);
            synchronized (objLock) {
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                    || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    boolean continueConsume = true;
                    for (int invokeCnt = 0; continueConsume; invokeCnt++) {
                        if (this.processQueue.isDropped()) {
                            LOG.warn("message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.removeConsumeRequest(this);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && !this.processQueue.isLocked()) {
                            LOG.warn("message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this, 10);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && this.processQueue.isLockExpired()) {
                            LOG.warn("message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this, 10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        if (invokeCnt > 0 && interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequest(this, true);
                            break;
                        }

                        // take messages by sharding key index
                        final int consumeBatchSize =
                            ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

                        List<MessageExt> msgs;
                        Object lock = consumeRequestLock.fetchLockObject(this.getMessageQueue(), this.shardingKeyIndex);
                        synchronized (lock) {
                            msgs = this.processQueue.takeMessagesByShardingKeyIndex(shardingKeyIndex, consumeBatchSize);

                            // no messages, return
                            if (msgs.isEmpty()) {
                                ConsumeMessageOrderlyService.this.removeConsumeRequest(this);
                                break;
                            }
                        }

                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

                        ConsumeOrderlyStatus status = null;
                        ConsumeMessageContext consumeMessageContext = null;
                        if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                            consumeMessageContext = new ConsumeMessageContext();
                            consumeMessageContext
                                .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
                            consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                            consumeMessageContext.setMq(messageQueue);
                            consumeMessageContext.setMsgList(msgs);
                            consumeMessageContext.setSuccess(false);
                            // init the consume context type
                            consumeMessageContext.setProps(new HashMap<String, String>());
                            ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                        }

                        boolean hasException = false;
                        long beginTimestamp = System.currentTimeMillis();
                        final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
                        try {
                            this.processQueue.getLockConsume().readLock().lock();
                            if (this.processQueue.isDropped()) {
                                LOG.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                    this.messageQueue);
                                ConsumeMessageOrderlyService.this.removeConsumeRequest(this);
                                break;
                            }

                            status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                        } catch (Throwable e) {
                            LOG.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                RemotingHelper.exceptionSimpleDesc(e),
                                ConsumeMessageOrderlyService.this.consumerGroup,
                                msgs,
                                messageQueue);
                            hasException = true;
                        } finally {
                            this.processQueue.getLockConsume().readLock().unlock();
                        }

                        if (null == status
                            || ConsumeOrderlyStatus.ROLLBACK == status
                            || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                            LOG.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                ConsumeMessageOrderlyService.this.consumerGroup,
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

                        if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                            consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                        }

                        if (null == status) {
                            status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }

                        if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                            consumeMessageContext.setStatus(status.toString());
                            consumeMessageContext
                                .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                            ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                        }

                        ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                            .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

                        continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
                    }
                } else {
                    if (this.processQueue.isDropped()) {
                        LOG.warn("message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        ConsumeMessageOrderlyService.this.removeConsumeRequest(this);
                        return;
                    }

                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this, 100);
                }
            }
        }

        @Override
        public int hashCode() {
            int hash = shardingKeyIndex;
            if (processQueue != null) {
                hash += processQueue.hashCode() * 31;
            }
            if (messageQueue != null) {
                hash += messageQueue.hashCode() * 31;
            }
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            ConsumeRequest other = (ConsumeRequest) obj;
            if (shardingKeyIndex != other.shardingKeyIndex) {
                return false;
            }

            if (processQueue != other.processQueue) {
                return false;
            }

            if (messageQueue == other.messageQueue) {
                return true;
            }
            if (messageQueue != null && messageQueue.equals(other.messageQueue)) {
                return true;
            }
            return false;
        }
    }

    private boolean isConsumeAccelerated(String topicName) {
        return this.defaultMQPushConsumerImpl.isConsumeAccelerated(topicName);
    }
}
