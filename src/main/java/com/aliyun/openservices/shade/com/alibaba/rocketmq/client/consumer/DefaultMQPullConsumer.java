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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.ClientConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.QueryResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.BoundaryType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ServiceState;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageDecoder;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.ExtraInfoUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RPCHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingException;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Default pulling consumer
 */
public class DefaultMQPullConsumer extends ClientConfig implements MQPullConsumer {
    protected final transient DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

    /**
     * Do the same thing for the same Group, the application must be set,and
     * guarantee Globally unique
     */
    protected String consumerGroup;
    /**
     * Long polling mode, the Consumer connection max suspend time, it is not
     * recommended to modify
     */
    private long brokerSuspendMaxTimeMillis = 1000 * 20;
    /**
     * Long polling mode, the Consumer connection timeout(must greater than
     * brokerSuspendMaxTimeMillis), it is not recommended to modify
     */
    private long consumerTimeoutMillisWhenSuspend = 1000 * 30;
    /**
     * The socket timeout in milliseconds
     */
    private long consumerPullTimeoutMillis = 1000 * 10;
    /**
     * Consumption pattern,default is clustering
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * Message queue listener
     */
    private MessageQueueListener messageQueueListener;
    /**
     * Offset Storage
     */
    private OffsetStore offsetStore;
    /**
     * Topic set you want to register
     */
    private Set<String> registerTopics = new HashSet<String>();
    /**
     * Queue allocation algorithm
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    private int maxReconsumeTimes = 16;

    /**
     * Whether auto update the topic route of pull.
     */
    private boolean autoUpdateTopicRoute = false;

    /**
     * Whether auto add subscription for pull topic.
     */
    private boolean autoAddSubscription = true;

    public DefaultMQPullConsumer() {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, null);
    }

    /**
     * Constructor specifying consumer group and RPC hook.
     *
     * @param consumerGroup
     * @param rpcHook
     */
    public DefaultMQPullConsumer(final String consumerGroup, RPCHook rpcHook) {
        this(null, consumerGroup, rpcHook);
    }

    /**
     * Constructor specifying namespace, consumer group and RPC hook.
     *
     * @param consumerGroup Consumer group.
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultMQPullConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.consumerGroup = consumerGroup;
        defaultMQPullConsumerImpl = new DefaultMQPullConsumerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying consumer group.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPullConsumer(final String consumerGroup) {
        this(null, consumerGroup, null);
    }

    /**
     *
     * Constructor specifying RPC hook.
     *
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultMQPullConsumer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPullConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return searchOffset(mq, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp, BoundaryType boundaryType) throws MQClientException {
        return this.defaultMQPullConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp, boundaryType);
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.maxOffset(queueWithNamespace(mq));
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.minOffset(queueWithNamespace(mq));
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    @Override
    public MessageExt viewMessage(String offsetMsgId) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        return this.defaultMQPullConsumerImpl.viewMessage(offsetMsgId);
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQPullConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public long getBrokerSuspendMaxTimeMillis() {
        return brokerSuspendMaxTimeMillis;
    }

    public void setBrokerSuspendMaxTimeMillis(long brokerSuspendMaxTimeMillis) {
        this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public long getConsumerPullTimeoutMillis() {
        return consumerPullTimeoutMillis;
    }

    public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
        this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
    }

    public long getConsumerTimeoutMillisWhenSuspend() {
        return consumerTimeoutMillisWhenSuspend;
    }

    public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend) {
        this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public MessageQueueListener getMessageQueueListener() {
        return messageQueueListener;
    }

    public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
        this.messageQueueListener = messageQueueListener;
    }

    public Set<String> getRegisterTopics() {
        return registerTopics;
    }

    public void setRegisterTopics(Set<String> registerTopics) {
        this.registerTopics = withNamespace(registerTopics);
    }

    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }

    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
    }

    @Override
    public void start() throws MQClientException {
        this.setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        this.defaultMQPullConsumerImpl.start();
    }

    @Override
    public void shutdown() {
        this.defaultMQPullConsumerImpl.shutdown();
    }

    @Override
    public void registerMessageQueueListener(String topic, MessageQueueListener listener) {
        synchronized (this.registerTopics) {
            this.registerTopics.add(withNamespace(topic));
            if (listener != null) {
                this.messageQueueListener = listener;
            }
        }
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums);
    }

    @Override
    public PullResult pull(MessageQueue mq, PullMessageSelector messageSelector)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector);
    }

    @Override
    public void pull(MessageQueue mq, PullMessageSelector messageSelector, PullCallback callback)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, callback);
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, timeout);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback,
        long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback, timeout);
    }

    @Override
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums);
    }

    @Override
    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums,
        PullCallback pullCallback)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback);
    }

    @Override
    public PullResult pullBlockIfNotFound(final MessageQueue mq, final PullMessageSelector messageSelector)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), messageSelector);
    }

    @Override
    public void pullBlockIfNotFound(final MessageQueue mq, final PullMessageSelector messageSelector, final PullCallback callback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), messageSelector, callback);
    }

    @Override
    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.defaultMQPullConsumerImpl.updateConsumeOffset(queueWithNamespace(mq), offset);
    }

    @Override
    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchConsumeOffset(queueWithNamespace(mq), fromStore);
    }

    @Override
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchMessageQueuesInBalance(withNamespace(topic));
    }

    @Override
    public MessageExt viewMessage(String topic, String uniqKey)
        throws InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(uniqKey);
            return this.viewMessage(uniqKey);
        } catch (Exception e) {
            // Ignore
        }
        return this.defaultMQPullConsumerImpl.queryMessageByUniqKey(withNamespace(topic), uniqKey);
    }

    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName, consumerGroup);
    }

    @Override
    public PopResult peekMessage(MessageQueue mq, int maxNums, String consumerGroup, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.peek(queueWithNamespace(mq), maxNums, withNamespace(consumerGroup), timeout);
    }

    @Override
    public PopResult pop(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pop(queueWithNamespace(mq), invisibleTime, maxNums, withNamespace(consumerGroup), timeout, initMode);
    }

    @Override
    public PopResult popOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.popOrderly(queueWithNamespace(mq), invisibleTime, maxNums, withNamespace(consumerGroup), timeout, initMode);
    }

    @Override
    public PopResult pop(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode, String expressionType, String expression)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pop(queueWithNamespace(mq), invisibleTime, maxNums, withNamespace(consumerGroup), timeout, initMode, expressionType, expression);
    }

    @Override
    public PopResult popOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup,
                                long timeout, int initMode, String expressionType, String expression)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.popOrderly(queueWithNamespace(mq), invisibleTime, maxNums, withNamespace(consumerGroup), timeout, initMode, expressionType, expression);
    }

    @Override
    public void popAsync(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.popAsync(queueWithNamespace(mq), invisibleTime, maxNums, withNamespace(consumerGroup), timeout, popCallback, poll, initMode);
    }

    @Override
    public void popAsync(MessageQueue mq, List<Integer> queueIdList, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.popAsync(queueWithNamespace(mq), queueIdList, invisibleTime, maxNums, withNamespace(consumerGroup), timeout, popCallback, poll, initMode);
    }

    @Override
    public void popAsyncOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup,
                                long timeout, PopCallback popCallback, boolean poll, int initMode)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.popAsyncOrderly(queueWithNamespace(mq), invisibleTime, maxNums, withNamespace(consumerGroup), timeout, popCallback, poll, initMode);
    }

    @Override
    public void popAsync(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode, String expressionType, String expression)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.popAsync(queueWithNamespace(mq), invisibleTime, maxNums, withNamespace(consumerGroup), timeout, popCallback, poll, initMode, expressionType, expression);
    }

    @Override
    public void popAsync(MessageQueue mq, List<Integer> queueIdList, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode, String expressionType, String expression)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.popAsync(queueWithNamespace(mq), queueIdList, invisibleTime, maxNums, withNamespace(consumerGroup), timeout, popCallback, poll, initMode, expressionType, expression);
    }

    @Override
    public void popAsyncOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup,
                                long timeout, PopCallback popCallback, boolean poll, int initMode, String expressionType, String expression)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.popAsyncOrderly(queueWithNamespace(mq), invisibleTime, maxNums, withNamespace(consumerGroup), timeout, popCallback, poll, initMode, expressionType, expression);
    }

    @Override
    public void notificationPollingAsync(MessageQueue mq, String consumerGroup, long timeout, NotificationCallback callback)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.notificationAsync(queueWithNamespace(mq), withNamespace(consumerGroup), timeout, callback);
    }

    @Override
    public void getPollingInfoAsync(MessageQueue mq, String consumerGroup, long timeout, PollingInfoCallback callback) 
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.getPollingNumAsync(queueWithNamespace(mq), withNamespace(consumerGroup), timeout, callback);
    }

    @Override
    public void peekAsync(MessageQueue mq, int maxNums, String consumerGroup, long timeout, PopCallback popCallback) 
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.peekAsync(queueWithNamespace(mq), maxNums, withNamespace(consumerGroup), timeout, popCallback);
    }

    @Override
    public void ackMessage(String topic, String consumerGroup, String extraInfo) 
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        MessageQueue mq = new MessageQueue(withNamespace(topic), ExtraInfoUtil.getBrokerName(extraInfoStrs), ExtraInfoUtil.getQueueId(extraInfoStrs));
        this.defaultMQPullConsumerImpl.ack(mq, ExtraInfoUtil.getQueueOffset(extraInfoStrs), withNamespace(consumerGroup), extraInfo);
    }

    @Override
    public void ackMessageAsync(String topic, String consumerGroup, String extraInfo, long timeOut, AckCallback callback) 
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        MessageQueue mq = new MessageQueue(withNamespace(topic), ExtraInfoUtil.getBrokerName(extraInfoStrs), ExtraInfoUtil.getQueueId(extraInfoStrs));
        this.defaultMQPullConsumerImpl.ackAsync(mq, ExtraInfoUtil.getQueueOffset(extraInfoStrs), withNamespace(consumerGroup), extraInfo, timeOut, callback);
    }

    @Override
    public void changeInvisibleTimeAsync(String topic, String consumerGroup, String extraInfo, long invisibleTime, long timeoutMillis, AckCallback callback) 
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        MessageQueue mq = new MessageQueue(withNamespace(topic), ExtraInfoUtil.getBrokerName(extraInfoStrs), ExtraInfoUtil.getQueueId(extraInfoStrs));
        this.defaultMQPullConsumerImpl.changeInvisibleTimeAsync(mq, ExtraInfoUtil.getQueueOffset(extraInfoStrs), withNamespace(consumerGroup), extraInfo, invisibleTime, timeoutMillis, callback);
    }

    @Override
    public void statisticsMessages(MessageQueue mq, String consumerGroup, long fromTime, long toTime, long timeout, StatisticsMessagesCallback callback)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQPullConsumerImpl.statisticsMessages(queueWithNamespace(mq), withNamespace(consumerGroup), fromTime, toTime, timeout, callback);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public DefaultMQPullConsumerImpl getDefaultMQPullConsumerImpl() {
        return defaultMQPullConsumerImpl;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public EventLoopGroup getEventLoopGroup() {
        return this.defaultMQPullConsumerImpl.getEventLoopGroup();
    }

    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) throws MQClientException {
        if (this.defaultMQPullConsumerImpl.getServiceState() != ServiceState.CREATE_JUST) {
            throw new MQClientException("The consumer service state not OK", null);
        }
        this.defaultMQPullConsumerImpl.setEventLoopGroup(eventLoopGroup);
    }

    public EventExecutorGroup getEventExecutorGroup() {
        return this.defaultMQPullConsumerImpl.getEventExecutorGroup();
    }

    public void setEventExecutorGroup(EventExecutorGroup eventExecutorGroup) throws MQClientException {
        if (this.defaultMQPullConsumerImpl.getServiceState() != ServiceState.CREATE_JUST) {
            throw new MQClientException("The consumer service state not OK", null);
        }
        this.defaultMQPullConsumerImpl.setEventExecutorGroup(eventExecutorGroup);
    }

    public boolean isAutoUpdateTopicRoute() {
        return autoUpdateTopicRoute;
    }

    public void setAutoUpdateTopicRoute(boolean autoUpdateTopicRoute) {
        this.autoUpdateTopicRoute = autoUpdateTopicRoute;
    }

    public boolean isAutoAddSubscription() {
        return autoAddSubscription;
    }

    public void setAutoAddSubscription(boolean autoAddSubscription) {
        this.autoAddSubscription = autoAddSubscription;
    }
}
