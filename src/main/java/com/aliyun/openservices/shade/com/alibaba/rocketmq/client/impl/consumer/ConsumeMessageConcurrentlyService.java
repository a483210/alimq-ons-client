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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.batch.TopicCache;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.*;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.stat.ConsumerStatsManager;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.Pair;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body.CMResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.google.common.base.Strings;

import java.util.*;
import java.util.concurrent.*;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    private volatile boolean stopped = false;
    private Thread batchConsumeThread;
    private final Object batchConsumeConditionVariable;

    private final ConcurrentMap<String, TopicCache> cachedMessages;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));

        if (this.defaultMQPushConsumer.getMaxBatchConsumeWaitTime() > 0) {
            this.batchConsumeThread = new Thread(new BatchConsumeTask());
            log.info("Consume message in batch mode, maxBatchAwaitTime={}ms", this.defaultMQPushConsumer.getMaxBatchConsumeWaitTime());
        }
        this.batchConsumeConditionVariable = new Object();
        this.cachedMessages = new ConcurrentHashMap<String, TopicCache>();
    }

    private class BatchConsumeTask implements Runnable {
        @Override
        public void run() {
            while (!stopped) {
                try {
                    tryBatchConsume();
                    synchronized (batchConsumeConditionVariable) {
                        batchConsumeConditionVariable.wait(1000);
                    }
                } catch (Throwable e) {
                    log.warn("Exception raised while schedule managing batch consuming", e);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ignore) {
                    }
                }
            }
        }
    }

    @Override
    public void start() {
        if (this.defaultMQPushConsumer.getMaxBatchConsumeWaitTime() > 0) {
            batchConsumeThread.start();
        }
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("Start to clean expired messages from tree-map");
                    cleanExpireMsg();
                    log.info("End of expired-message cleaning");
                } catch (Throwable e) {
                    log.error("[BUG]Should NEVER reach here", e);
                }
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        if (null != batchConsumeThread) {
            batchConsumeThread.interrupt();
        }

        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
        this.cleanExpireMsgExecutors.shutdown();
    }

    void tryBatchConsume() {
        for (Map.Entry<String, TopicCache> entry : cachedMessages.entrySet()) {
            while (true) {
                int size = entry.getValue().size();
                if (size >= this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize()
                    || entry.getValue().elapsed() >= this.defaultMQPushConsumer.getMaxBatchConsumeWaitTime()) {
                    List<MessageExt> messages = new ArrayList<MessageExt>();
                    int amount = Math.min(size, this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize());
                    // Note messages are appended chronologically by the time they are decoded, such that earliest
                    // fetched message will be consumed first.
                    entry.getValue().take(amount, messages);
                    TopicBatchConsumeRequest consumeRequest = new TopicBatchConsumeRequest(messages);
                    consumeExecutor.submit(consumeRequest);
                } else {
                    break;
                }
            }
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
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // + 1);
        // }
        // log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public void decCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // - 1);
        // }
        // log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
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
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyByQueueContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue) {
        if (msgs == null || msgs.isEmpty()) {
            return;
        }

        if (this.defaultMQPushConsumer.getMaxBatchConsumeWaitTime() > 0) {
            // Restore bare topic
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            // Topic with namespace and retry prefix stripped
            String topic = msgs.iterator().next().getTopic();
            if (!cachedMessages.containsKey(topic)) {
                cachedMessages.putIfAbsent(topic, new TopicCache(topic));
            }
            cachedMessages.get(topic).put(msgs);
            synchronized (batchConsumeConditionVariable) {
                batchConsumeConditionVariable.notify();
            }
            return;
        }

        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            try {
                Map.Entry<MessageQueue, ProcessQueue> next = it.next();
                ProcessQueue pq = next.getValue();
                pq.cleanExpiredMsg(this.defaultMQPushConsumer);
            } catch (Throwable e) {
                log.warn("Unexpected exception raised when trying to clean expired message from tree-map", e);
            }
        }
    }

    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty()) {
            return;
        }

        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    if (context.getCheckSendBackHook() != null &&
                        !context.getCheckSendBackHook().needSendBack(msg, context)) {
                        continue;
                    }
                    if (!consumeRequest.getProcessQueue().hasMessage(msg)) {
                        /* msg no longer exists in process queue, which means it has been sent back and cleaned
                         * due to consumer timeout.
                         */
                        log.info("Msg not exists in process queue, skip send back, offset id: {}", msg.getMsgId());
                        continue;
                    }
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private static String topicBrokerNameOf(final MessageExt messageExt) {
        String topic;
        if (Strings.isNullOrEmpty(messageExt.getProperty(MessageConst.PROPERTY_FQN_TOPIC))) {
            topic = messageExt.getTopic();
        } else {
            topic = messageExt.getProperty(MessageConst.PROPERTY_FQN_TOPIC);
        }
        return topic + "@" + messageExt.getBrokerName();
    }

    public static String topicOf(final MessageExt messageExt) {
        String fqn = messageExt.getProperty(MessageConst.PROPERTY_FQN_TOPIC);
        if (Strings.isNullOrEmpty(fqn)) {
            return messageExt.getTopic();
        }
        return fqn;
    }

    class TopicBatchConsumeRequest implements Runnable {
        private final List<MessageExt> messages;

        private String topic;

        public TopicBatchConsumeRequest(List<MessageExt> messages) {
            this.messages = messages;
            if (null != this.messages && !this.messages.isEmpty()) {
                this.topic = this.messages.iterator().next().getTopic();
            }
        }

        private String msgIdsOf(Collection<MessageExt> messages) {
            StringBuilder sb = new StringBuilder();
            if (null == messages || messages.isEmpty()) {
                return sb.toString();
            }

            for (MessageExt message : messages) {
                if (0 == sb.length()) {
                    sb.append(message.getMsgId());
                } else {
                    sb.append(',').append(message.getMsgId());
                }
            }
            return sb.toString();
        }

        @Override
        public void run() {
            if (messages.isEmpty() || null == topic) {
                return;
            }
            try {
                ConsumeConcurrentlyStatus status = null;
                // Segregate messages by message queue.
                Map<Pair<String, Integer>, List<MessageExt>> groupBy = new HashMap<Pair<String, Integer>,
                    List<MessageExt>>();
                for (MessageExt message : messages) {
                    Pair<String, Integer> key = new Pair<String, Integer>(topicBrokerNameOf(message), message.getQueueId());
                    if (!groupBy.containsKey(key)) {
                        groupBy.put(key, new ArrayList<MessageExt>());
                    }
                    groupBy.get(key).add(message);
                }

                Map<MessageQueue, ConsumeMessageContext> consumeMessageContextMap = new HashMap<MessageQueue,
                    ConsumeMessageContext>();

                for (Map.Entry<Pair<String, Integer>, List<MessageExt>> entry : groupBy.entrySet()) {
                    MessageExt headMessage = entry.getValue().iterator().next();
                    assert null != headMessage;
                    MessageQueue messageQueue = new MessageQueue(topicOf(headMessage), headMessage.getBrokerName(),
                        entry.getKey().getObject2());
                    ConsumeMessageContext consumeMessageContext;
                    if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                        consumeMessageContext = new ConsumeMessageContext();
                        consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                        consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                        consumeMessageContext.setProps(new HashMap<String, String>());
                        consumeMessageContext.setMq(messageQueue);
                        consumeMessageContext.setMsgList(entry.getValue());
                        consumeMessageContext.setSuccess(false);
                        consumeMessageContextMap.put(messageQueue, consumeMessageContext);
                        ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                    }
                }

                long beginTimestamp = System.currentTimeMillis();
                boolean hasException = false;
                ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                ConsumeConcurrentlyContext context = new ConsumeConcurrentlyByTopicContext();
                try {
                    if (!messages.isEmpty()) {
                        for (MessageExt msg : messages) {
                            // Update message consume start time
                            MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                        }
                    }
                    status =
                        ConsumeMessageConcurrentlyService.this.messageListener.consumeMessage(Collections.unmodifiableList(messages),
                            context);
                } catch (Throwable e) {
                    log.warn("consumeMessage exception: {} Group: {} Msgs: {}",
                        RemotingHelper.exceptionSimpleDesc(e),
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        messages);
                    hasException = true;
                }
                long consumeRT = System.currentTimeMillis() - beginTimestamp;
                if (null == status) {
                    if (hasException) {
                        returnType = ConsumeReturnType.EXCEPTION;
                    } else {
                        returnType = ConsumeReturnType.RETURNNULL;
                    }
                } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                    returnType = ConsumeReturnType.TIME_OUT;
                } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                    returnType = ConsumeReturnType.FAILED;
                } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                    returnType = ConsumeReturnType.SUCCESS;
                }

                if (null == status) {
                    log.warn("consumeMessage return null, Group: {} Msgs: {}",
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        messages);
                    status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                Map<MessageQueue, Integer> ackMap = new HashMap<MessageQueue, Integer>();
                if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS != status) {
                    for (int i = 0; i < context.getAckIndex(); i++) {
                        if (i >= messages.size()) {
                            break;
                        }
                        MessageExt message = messages.get(i);
                        MessageQueue messageQueue = new MessageQueue(topic, message.getBrokerName(), message.getQueueId());
                        if (!ackMap.containsKey(messageQueue)) {
                            ackMap.put(messageQueue, 0);
                        }
                        ackMap.put(messageQueue, ackMap.get(messageQueue) + 1);
                    }
                }

                for (Map.Entry<Pair<String, Integer>, List<MessageExt>> entry : groupBy.entrySet()) {
                    try {
                        MessageExt headMessage = entry.getValue().iterator().next();
                        assert null != headMessage;
                        MessageQueue messageQueue = new MessageQueue(topicOf(headMessage), headMessage.getBrokerName(),
                            entry.getKey().getObject2());
                        ConsumeConcurrentlyContext queueContext = new ConsumeConcurrentlyByQueueContext(messageQueue);
                        if (ackMap.containsKey(messageQueue)) {
                            queueContext.setAckIndex(ackMap.get(messageQueue));
                        }
                        ConsumeMessageContext consumeMessageContext = consumeMessageContextMap.get(messageQueue);
                        if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                            consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            consumeMessageContext.getProps().put(MixAll.CONSUME_EXACTLYONCE_STATUS, queueContext.getExactlyOnceStatus().name());
                            consumeMessageContext.setStatus(status.toString());
                            consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                            ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                        }
                        ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                            .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
                        ProcessQueue processQueue =
                            defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().get(messageQueue);
                        if (null != processQueue && !processQueue.isDropped()) {
                            ConsumeRequest consumeRequest = new ConsumeRequest(entry.getValue(), processQueue, messageQueue);
                            ConsumeMessageConcurrentlyService.this.processConsumeResult(status, queueContext, consumeRequest);
                        } else {
                            log.info("processQueue is dropped without process consume result. messageQueue={}, " +
                                    "msgIdList={}", messageQueue, msgIdsOf(entry.getValue()));
                        }
                    } catch (Throwable e) {
                        log.error("[BUG]Unexpected exception raised when post-process mini-batch", e);
                    }
                }
            } catch (Throwable e) {
                log.error("[BUG]TopicBatchConsumeRequest raised an unexpected exception", e);
            }
        }
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyByQueueContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
            }
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                consumeMessageContext.getProps().put(MixAll.CONSUME_EXACTLYONCE_STATUS, context.getExactlyOnceStatus().name());
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
