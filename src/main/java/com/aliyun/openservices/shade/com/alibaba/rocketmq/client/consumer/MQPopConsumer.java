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

import java.util.List;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingException;

/**
 * pop&ack consumer interface
 */
public interface MQPopConsumer {


    /**
     * peek message in the specified timeout without change any status
     *
     * @param mq      queueId is -1,  peek message in any queue from broker; else peek message in the specified queue from broker.
     * @param maxNums
     * @param timeout
     * @return msg list
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult peek(MessageQueue mq, int maxNums, String consumerGroup, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * ack msg in oneway , broker will commit this offset
     *
     * @param mq
     * @param offset
     * @param consumerGroup
     * @param extraInfo
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void ack(final MessageQueue mq, final long offset,
             String consumerGroup, String extraInfo) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


    /**
     * async pop message in the specified timeout,  broker maintains the consume offset.
     *
     * @param mq            queueId is -1,  pop message in any queue from broker; else pop message in the specified queue from broker.
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
     * async pop message in the specified timeout orderly,  broker maintains the consume offset.
     *
     * @param mq            pop message in the specified queue from broker.
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
     * async pop message in the specified timeout,  broker maintains the consume offset.
     *
     * @param mq             queueId is -1,  pop message in any queue from broker; else pop message in the specified queue from broker.
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
     * @param expressionType @see {@link com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType}
     * @param expression     filter expression
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
     * async pop message in the specified timeout orderly,  broker maintains the consume offset.
     *
     * @param mq             pop message in the specified queue from broker.
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
     * @param expressionType @see {@link com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType}
     * @param expression     filter expression
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void popAsyncOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, PopCallback popCallback, boolean poll, int initMode,
                  String expressionType, String expression) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


    /**
     * async peek message in the specified timeout without change any status
     *
     * @param mq            queueId is -1,  peek message in any queue from broker; else peek message in the specified queue from broker.
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
     * pop message in the specified timeout,  broker maintains the consume offset.
     *
     * @param mq            queueId is -1,  pop message in any queue from broker; else pop message in the specified queue from broker.
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @return msg list
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult pop(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop message in the specified timeout orderly,  broker maintains the consume offset.
     *
     * @param mq            pop message in the specified queue from broker.
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @return msg list
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult popOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop message in the specified timeout,  broker maintains the consume offset.
     *
     * @param mq             queueId is -1,  pop message in any queue from broker; else pop message in the specified queue from broker.
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param expressionType @see {@link com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType}
     * @param expression     filter expression
     * @return msg list
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult pop(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode, String expressionType, String expression)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * pop message in the specified timeout orderly,  broker maintains the consume offset.
     *
     * @param mq             pop message in the specified queue from broker.
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param expressionType @see {@link com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType}
     * @param expression     filter expression
     * @return msg list
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    PopResult popOrderly(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout, int initMode, String expressionType, String expression)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * async ack msg , broker will commit this offset
     *
     * @param mq
     * @param offset
     * @param consumerGroup
     * @param extraInfo
     * @param timeOut
     * @param callback
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    void ackAsync(MessageQueue mq, long offset, String consumerGroup, String extraInfo, long timeOut, AckCallback callback)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * change invisibleTime of specified msg
     *
     * @param mq
     * @param offset
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
    void changeInvisibleTimeAsync(MessageQueue mq, long offset, String consumerGroup, String extraInfo, long invisibleTime, long timeoutMillis, AckCallback callback)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


    /**
     * Statistics data of topic for consumer
     *
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

    void notificationAsync(MessageQueue mq, String consumerGroup, long timeout, NotificationCallback callback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void getPollingNumAsync(MessageQueue mq, String consumerGroup, long timeout, PollingInfoCallback callback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

}
