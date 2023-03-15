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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.ClientConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.QueryResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.reporter.ConsumerStatusReporter;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.BoundaryType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ServiceState;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageDecoder;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RPCHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingException;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * In most scenarios, this is the mostly recommended class to consume messages.
 * </p>
 *
 * Technically speaking, this push client is virtually a wrapper of the underlying pull service. Specifically, on
 * arrival of messages pulled from brokers, it roughly invokes the registered callback handler to feed the messages.
 * </p>
 *
 * See quickstart/Consumer in the example module for a typical usage.
 * </p>
 *
 * <p>
 * <strong>Thread Safety:</strong> After initialization, the instance can be regarded as thread-safe.
 * </p>
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {

    /**
     * Internal implementation. Most of the functions herein are delegated to it.
     */
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     * </p>
     *
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     */
    private String consumerGroup;

    /**
     * Message model defines the way how messages are delivered to each consumer clients.
     * </p>
     *
     * RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
     * the same {@link #consumerGroup} would only consume shards of the messages subscribed, which achieves load
     * balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
     * separately.
     * </p>
     *
     * This field defaults to clustering.
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Consuming point on consumer booting.
     * </p>
     *
     * There are three consuming points:
     * <ul>
     * <li>
     * <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
     * If it were a newly booting up consumer client, according aging of the consumer group, there are two
     * cases:
     * <ol>
     * <li>
     * if the consumer group is created so recently that the earliest message being subscribed has yet
     * expired, which means the consumer group represents a lately launched business, consuming will
     * start from the very beginning;
     * </li>
     * <li>
     * if the earliest message being subscribed has expired, consuming will start from the latest
     * messages, meaning messages born prior to the booting timestamp would be ignored.
     * </li>
     * </ol>
     * </li>
     * <li>
     * <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
     * </li>
     * <li>
     * <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
     * messages born prior to {@link #consumeTimestamp} will be ignored
     * </li>
     * </ul>
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * Backtracking consumption time with second precision. Time format is
     * 20131223171201<br>
     * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
     * Default backtracking consumption time Half an hour ago.
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

    /**
     * Queue allocation algorithm specifying how message queues are allocated to each consumer clients.
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * Subscription relationship
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();

    /**
     * Message listener
     */
    private MessageListener messageListener;

    /**
     * Consumer status reporter
     */
    private ConsumerStatusReporter consumerStatusReporter;

    /**
     * Offset Storage
     */
    private OffsetStore offsetStore;

    /**
     * Minimum consumer thread number
     */
    private int consumeThreadMin = 20;

    /**
     * Max consumer thread number
     */
    private int consumeThreadMax = 64;

    /**
     * Threshold for dynamic adjustment of the number of thread pool
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * Concurrently max span offset.it has no effect on sequential consumption
     */
    private int consumeConcurrentlyMaxSpan = 2000;

    /**
     * Flow control threshold on queue level, each message queue will cache at most 1000 messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     */
    private int pullThresholdForQueue = 1000;

    /**
     * Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     *
     * <p>
     * The size of a message only measured by message body, so it's not accurate
     */
    private int pullThresholdSizeForQueue = 100;

    /**
     * Flow control threshold on topic level, default value is -1(Unlimited)
     * <p>
     * The value of {@code pullThresholdForQueue} will be overwrote and calculated based on
     * {@code pullThresholdForTopic} if it is't unlimited
     * <p>
     * For example, if the value of pullThresholdForTopic is 1000 and 10 message queues are assigned to this consumer,
     * then pullThresholdForQueue will be set to 100
     */
    private int pullThresholdForTopic = -1;

    /**
     * Limit the cached message size on topic level, default value is -1 MiB(Unlimited)
     * <p>
     * The value of {@code pullThresholdSizeForQueue} will be overwrote and calculated based on
     * {@code pullThresholdSizeForTopic} if it is't unlimited
     * <p>
     * For example, if the value of pullThresholdSizeForTopic is 1000 MiB and 10 message queues are
     * assigned to this consumer, then pullThresholdSizeForQueue will be set to 100 MiB
     */
    private int pullThresholdSizeForTopic = -1;

    /**
     * Message pull Interval
     */
    private long pullInterval = 0;

    /**
     * Batch consumption size
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * Maximum amount of time in milliseconds to wait before group-consume
     */
    private long maxBatchConsumeWaitTime = 0;

    /**
     * Batch pull size
     */
    private int pullBatchSize = 32;

    /**
     * Whether update subscription relationship when every pull
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    /**
     * Max re-consume times. -1 means 16 times.
     * </p>
     *
     * If messages are re-consumed more than {@link #maxReconsumeTimes} before success, it's be directed to a deletion
     * queue waiting.
     */
    private int maxReconsumeTimes = -1;

    /**
     * Suspending pulling time for cases requiring slow pulling like flow-control scenario.
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    private long consumeTimeout = 15;

    /**
     * Whether enable accelerator of orderly consumption which consume messages concurrently with different sharding
     * keys in the same queue
     */
    private boolean orderlyConsumeAccelerator = false;

    /**
     * Max concurrency for a queue of order topic.
     * <p>
     * If bigger than 1, consumer would process message concurrently in a queue among different sharding keys on the promise
     * that messages with the same sharding key should be consumed orderly. Other wise, it would process orderly.
     *
     * <p>
     * Only available when orderlyConsumeAccelerator is true
     */
    private int maxConcurrencyForOrderQueue = 16;

    private boolean useConsumeOrderlyByGroupService = false;

    /**
     * Default constructor.
     */
    public DefaultMQPushConsumer() {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, null, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying namespace and consumer group.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup) {
        this(namespace, consumerGroup, null, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying namespace, consumer group and RPC hook .
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param consumerGroup Consumer group.
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
        this(namespace, consumerGroup, rpcHook, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group, RPC hook and message queue allocating algorithm.
     *
     * @param consumerGroup Consume queue.
     * @param rpcHook RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy message queue allocating algorithm.
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this(null, consumerGroup, rpcHook, allocateMessageQueueStrategy);
    }

    /**
     * Constructor specifying namespace, consumer group, RPC hook and message queue allocating algorithm.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param consumerGroup Consumer group.
     * @param rpcHook RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy message queue allocating algorithm.
     */
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.consumerGroup = consumerGroup;
        this.namespace = namespace;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying RPC hook.
     *
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultMQPushConsumer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPushConsumer(final String consumerGroup) {
        this(null, consumerGroup, null, new AllocateMessageQueueAveragely());
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPushConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return searchOffset(mq, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp, BoundaryType boundaryType) throws MQClientException {
        return this.defaultMQPushConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp, boundaryType);
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.maxOffset(queueWithNamespace(mq));
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.minOffset(queueWithNamespace(mq));
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    @Override
    public MessageExt viewMessage(
        String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQPushConsumerImpl.viewMessage(offsetMsgId);
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQPushConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    @Override
    public MessageExt viewMessage(String topic, String msgId)
        throws InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
            // Ignore
        }
        return this.defaultMQPushConsumerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }

    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public long getMaxBatchConsumeWaitTime() {
        return maxBatchConsumeWaitTime;
    }

    public void setMaxBatchConsumeWaitTime(long amount, TimeUnit timeUnit) throws MQClientException {
        if (amount < 0) {
            return;
        }

        // Treat null time-unit as TimeUnit.MILLISECONDS
        if (null == timeUnit) {
            this.maxBatchConsumeWaitTime = amount;
            return;
        }

        this.maxBatchConsumeWaitTime = TimeUnit.MILLISECONDS.convert(amount, timeUnit);

        if (this.maxBatchConsumeWaitTime  > TimeUnit.MILLISECONDS.convert(consumeTimeout, TimeUnit.MINUTES) / 2) {
            throw new MQClientException(ResponseCode.BAD_CONFIGURATION, "Batch await time should not exceed half of " +
                "consume timeout");
        }
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    /**
     * Set maximum thread count of consume thread pool.
     * <p>
     * Also, if consumeThreadMax is less than consumeThreadMin, set consumeThreadMin equal to consumeThreadMax.
     * </p>
     * @param consumeThreadMax
     */
    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
        if (this.consumeThreadMax < this.consumeThreadMin) {
            this.consumeThreadMin = consumeThreadMax;
        }
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    /**
     * Set minimum thread count of consume thread pool.
     * <p>
     * Also, if consumeThreadMax is less than consumeThreadMin, set consumeThreadMax equal to consumeThreadMin.
     * </p>
     * @param consumeThreadMin
     */
    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
        if (this.consumeThreadMax < this.consumeThreadMin) {
            this.consumeThreadMax = consumeThreadMin;
        }
    }

    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public ConsumerStatusReporter getConsumerStatusReporter() {
        return consumerStatusReporter;
    }

    public void setConsumerStatusReporter(
        ConsumerStatusReporter consumerStatusReporter) {
        this.consumerStatusReporter = consumerStatusReporter;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPullThresholdForTopic() {
        return pullThresholdForTopic;
    }

    public void setPullThresholdForTopic(final int pullThresholdForTopic) {
        this.pullThresholdForTopic = pullThresholdForTopic;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(final int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public int getPullThresholdSizeForTopic() {
        return pullThresholdSizeForTopic;
    }

    public void setPullThresholdSizeForTopic(final int pullThresholdSizeForTopic) {
        this.pullThresholdSizeForTopic = pullThresholdSizeForTopic;
    }

    public Map<String, String> getSubscription() {
        return subscription;
    }

    public void setSubscription(Map<String, String> subscription) {
        Map<String, String> subscriptionWithNamespace = new HashMap<String, String>();
        for (String topic : subscription.keySet()) {
            subscriptionWithNamespace.put(withNamespace(topic), subscription.get(topic));
        }
        this.subscription = subscriptionWithNamespace;
    }

    /**
     * Send message back to broker which will be re-delivered in future.
     *
     * @param msg Message to send back.
     * @param delayLevel delay level.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, msg.getBrokerName());
    }

    /**
     * Send message back to the broker whose name is <code>brokerName</code> and the message will be re-delivered in
     * future.
     *
     * @param msg Message to send back.
     * @param delayLevel delay level.
     * @param brokerName broker name.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
    }

    /**
     * This method gets internal infrastructure readily to serve. Instances must call this method after configuration.
     *
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void start() throws MQClientException {
        setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        this.defaultMQPushConsumerImpl.start();
    }

    /**
     * Shut down this client and releasing underlying resources.
     */
    @Override
    public void shutdown() {
        this.defaultMQPushConsumerImpl.shutdown();
    }

    @Override
    @Deprecated
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Register a callback to execute on message arrival for concurrent consuming.
     *
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Register a callback to execute on message arrival for orderly consuming.
     *
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Subscribe a topic to consuming subscription.
     *
     * @param topic topic to subscribe.
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     * if null or * expression,meaning subscribe all
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), subExpression);
    }

    /**
     * Subscribe a topic to consuming subscription.
     *
     * @param topic topic to consume.
     * @param fullClassName full class name,must extend com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter. MessageFilter
     * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety
     */
    @Override
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), fullClassName, filterClassSource);
    }

    /**
     * Subscribe a topic by message selector.
     *
     * @param topic topic to consume.
     * @param messageSelector {@link com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector}
     * @see com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector#bySql
     * @see com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector#byTag
     */
    @Override
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), messageSelector);
    }

    /**
     * Un-subscribe the specified topic from subscription.
     *
     * @param topic message topic
     */
    @Override
    public void unsubscribe(String topic) {
        this.defaultMQPushConsumerImpl.unsubscribe(withNamespace(topic));
    }

    /**
     * Update the message consuming thread core pool size.
     *
     * @param corePoolSize new core pool size.
     */
    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
    }

    @Override
    public void allowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) {
        this.defaultMQPushConsumerImpl.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
    }

    /**
     * Suspend pulling new messages.
     */
    @Override
    public void suspend() {
        this.defaultMQPushConsumerImpl.suspend();
    }

    /**
     * Resume pulling.
     */
    @Override
    public void resume() {
        this.defaultMQPushConsumerImpl.resume();
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    public boolean isPostSubscriptionWhenPull() {
        return postSubscriptionWhenPull;
    }

    public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
        this.postSubscriptionWhenPull = postSubscriptionWhenPull;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public long getAdjustThreadPoolNumsThreshold() {
        return adjustThreadPoolNumsThreshold;
    }

    public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
        this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(final long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(final long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public boolean isOrderlyConsumeAccelerator() {
        return orderlyConsumeAccelerator;
    }

    public void setOrderlyConsumeAccelerator(boolean orderlyConsumeAccelerator) {
        this.orderlyConsumeAccelerator = orderlyConsumeAccelerator;
    }

    public int getMaxConcurrencyForOrderQueue() {
        return maxConcurrencyForOrderQueue;
    }

    public void setMaxConcurrencyForOrderQueue(int maxConcurrencyForOrderQueue) {
        this.maxConcurrencyForOrderQueue = maxConcurrencyForOrderQueue;
    }

    public EventLoopGroup getEventLoopGroup() {
        return this.defaultMQPushConsumerImpl.getEventLoopGroup();
    }

    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) throws MQClientException {
        if (this.defaultMQPushConsumerImpl.getServiceState() != ServiceState.CREATE_JUST) {
            throw new MQClientException("The consumer service state not OK", null);
        }
        this.defaultMQPushConsumerImpl.setEventLoopGroup(eventLoopGroup);
    }

    public EventExecutorGroup getEventExecutorGroup() {
        return this.defaultMQPushConsumerImpl.getEventExecutorGroup();
    }

    public void setEventExecutorGroup(EventExecutorGroup eventExecutorGroup) throws MQClientException {
        if (this.defaultMQPushConsumerImpl.getServiceState() != ServiceState.CREATE_JUST) {
            throw new MQClientException("The consumer service state not OK", null);
        }
        this.defaultMQPushConsumerImpl.setEventExecutorGroup(eventExecutorGroup);
    }

    public boolean isUseConsumeOrderlyByGroupService() {
        return useConsumeOrderlyByGroupService;
    }

    public void setUseConsumeOrderlyByGroupService(boolean useConsumeOrderlyByGroupService) {
        this.useConsumeOrderlyByGroupService = useConsumeOrderlyByGroupService;
    }
}
