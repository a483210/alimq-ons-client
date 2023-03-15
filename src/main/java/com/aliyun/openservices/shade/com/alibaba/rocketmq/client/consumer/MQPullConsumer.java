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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.util.List;
import java.util.Set;

/**
 * Pulling consumer interface
 */
public interface MQPullConsumer extends MQConsumer {
    /**
     * Start the consumer
     */
    void start() throws MQClientException;

    /**
     * Shutdown the consumer
     */
    void shutdown();

    /**
     * Register the message queue listener
     */
    void registerMessageQueueListener(final String topic, final MessageQueueListener listener);

    /**
     * Pulling the messages,not blocking
     *
     * @param mq            from which message queue
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br> if
     *                      null or * expression,meaning subscribe
     *                      all
     * @param offset        from where to pull
     * @param maxNums       max pulling numbers
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset,
                    final int maxNums) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    /**
     * Pulling the messages with MessageSelector in a sync. way
     *
     * @param mq              from which message queue
     * @param messageSelector message selector
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PullResult pull(final MessageQueue mq, final PullMessageSelector messageSelector)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * Pulling the messages with MessageSelector and PullCallback in an async. way
     *
     * @param mq              from which message queue
     * @param messageSelector message selector
     * @param callback        callback, can not be null
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void pull(final MessageQueue mq, final PullMessageSelector messageSelector, final PullCallback callback)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * Pulling the messages in the specified timeout
     *
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset,
                    final int maxNums, final long timeout) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException;

    /**
     * Pulling the messages in a async. way
     */
    void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
              final PullCallback pullCallback) throws MQClientException, RemotingException,
        InterruptedException;

    /**
     * Pulling the messages in a async. way
     */
    void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
              final PullCallback pullCallback, long timeout) throws MQClientException, RemotingException,
        InterruptedException;


    /**
     * Pulling the messages,if no message arrival,blocking some time
     *
     * @return The resulting {@code PullRequest}
     */
    PullResult pullBlockIfNotFound(final MessageQueue mq, final String subExpression,
                                   final long offset, final int maxNums) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;

    /**
     * Pulling the messages through callback function,if no message arrival,blocking.
     */
    void pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset,
                             final int maxNums, final PullCallback pullCallback) throws MQClientException, RemotingException,
            InterruptedException;


    /**
     * Pulling the messages,if no message arrival,blocking some time
     *
     * @return The resulting {@code PullRequest}
     */
    PullResult pullBlockIfNotFound(final MessageQueue mq, final PullMessageSelector messageSelector) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;

    /**
     * Pulling the messages through callback function,if no message arrival,blocking.
     */
    void pullBlockIfNotFound(final MessageQueue mq, final PullMessageSelector messageSelector, final PullCallback callback) throws MQClientException, RemotingException,
            InterruptedException;




    /**
     * Update the offset
     */
    void updateConsumeOffset(final MessageQueue mq, final long offset) throws MQClientException;

    /**
     * Fetch the offset
     *
     * @return The fetched offset of given queue, return -1 if offset does not exist. Maybe return optional offset is
     * a better choice to indicate this case, but java6 does not introduce this feature unfortunately, or guava
     * {@link Optional} should be considered.
     */
    long fetchConsumeOffset(final MessageQueue mq, final boolean fromStore) throws MQClientException;

    /**
     * Fetch the message queues according to the topic
     *
     * @param topic message topic
     * @return message queue set
     */
    Set<MessageQueue> fetchMessageQueuesInBalance(final String topic) throws MQClientException;

    /**
     * If consuming failure,message will be send back to the broker,and delay consuming in some time later.<br>
     * Mind! message can only be consumed in the same group.
     */
    void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * Just get message for view
     *
     * @param mq
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult peekMessage(MessageQueue mq, int maxNums, String consumerGroup, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


    /**
     * pop async
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void popAsync(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void popAsync(MessageQueue mq, List<Integer> queueIdList, long invisibleTime, int maxNums, String consumerGroup,
        long timeout, PopCallback popCallback, boolean poll,
        int initMode) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop async orderly
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void popAsyncOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


    /**
     * pop async with filter expression
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
     * @param expressionType
     * @param expression
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void popAsync(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode,
                  String expressionType, String expression) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void popAsync(MessageQueue mq, List<Integer> queueIdList, long invisibleTime, int maxNums, String consumerGroup,
        long timeout, PopCallback popCallback, boolean poll, int initMode, String expressionType,
        String expression) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop async with filter expression orderly
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
     * @param expressionType
     * @param expression
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void popAsyncOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode,
                  String expressionType, String expression) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * peek async
     *
     * @param mq
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void peekAsync(MessageQueue mq, int maxNums, String consumerGroup, long timeout, PopCallback popCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop sync
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param initMode
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult pop(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop sync orderly
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param initMode
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult popOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop sync with expression filter
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param initMode
     * @param expressionType
     * @param expression
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult pop(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode, String expressionType, String expression)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop sync with expression filter orderly
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param initMode
     * @param expressionType
     * @param expression
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult popOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode, String expressionType, String expression)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * @param mq
     * @param consumerGroup
     * @param fromTime
     * @param toTime
     * @param timeout
     * @param callback
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void statisticsMessages(MessageQueue mq, String consumerGroup, long fromTime, long toTime, long timeout, StatisticsMessagesCallback callback)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * ack async, confirm the msg is consumed suc.
     *
     * @param topic
     * @param consumerGroup
     * @param extraInfo
     * @param timeOut
     * @param callback
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void ackMessageAsync(String topic, String consumerGroup, String extraInfo, long timeOut, AckCallback callback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * change retry time async.
     *
     * @param topic
     * @param consumerGroup
     * @param extraInfo
     * @param invisibleTime
     * @param timeoutMillis
     * @param callback
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void changeInvisibleTimeAsync(String topic, String consumerGroup, String extraInfo, long invisibleTime, long timeoutMillis, AckCallback callback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * ack sync
     *
     * @param topic
     * @param consumerGroup
     * @param extraInfo
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void ackMessage(String topic, String consumerGroup, String extraInfo) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void notificationPollingAsync(MessageQueue mq, String consumerGroup, long timeout, NotificationCallback callback)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * get polling num of broker.
     *
     * @param mq
     * @param consumerGroup
     * @param timeout
     * @param callback
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void getPollingInfoAsync(MessageQueue mq, String consumerGroup, long timeout, PollingInfoCallback callback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


}
