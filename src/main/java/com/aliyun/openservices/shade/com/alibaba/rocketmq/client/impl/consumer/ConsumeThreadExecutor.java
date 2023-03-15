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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.Pair;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;

public class ConsumeThreadExecutor {
    private final static InternalLogger LOG = ClientLogger.getLog();
    private final ThreadPoolExecutor threadPoolExecutor;
    private final MessageQueueGroupLock consumeRequestLock = new MessageQueueGroupLock();
    private final MessageQueueGroupLock shardingKeyLock = new MessageQueueGroupLock();
    private final MergeThreadExecutor mergeThreadExecutor;

    private ConsumeMessageOrderlyByGroupService cs;
    private ConcurrentHashMap<ConsumeRequest, ConsumeRequest> consumeRequestMap = new ConcurrentHashMap<ConsumeRequest, ConsumeRequest>();

    public ConsumeThreadExecutor(ConsumeMessageOrderlyByGroupService cs, MergeThreadExecutor mergeThreadExecutor) {
        this.cs = cs;
        this.mergeThreadExecutor = mergeThreadExecutor;
        this.threadPoolExecutor = new ThreadPoolExecutor(
            cs.getDefaultMQPushConsumer().getConsumeThreadMin(),
            cs.getDefaultMQPushConsumer().getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("ConsumeMessageThread_"));
    }

    public void shutdown() {
        this.threadPoolExecutor.shutdown();
    }

    public void setCorePoolSize(int corePoolSize) {
        mergeThreadExecutor.setCorePoolSize(corePoolSize / 2);
        this.threadPoolExecutor.setCorePoolSize(corePoolSize);
    }

    public int getCorePoolSize() {
        return this.threadPoolExecutor.getCorePoolSize();
    }

    public void allowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) {
        this.threadPoolExecutor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
    }

    public MessageQueueGroupLock getConsumeRequestLock() {
        return consumeRequestLock;
    }

    public MessageQueueGroupLock getShardingKeyLock() {
        return shardingKeyLock;
    }

    public void remove(ConsumeRequest consumeRequest) {
        consumeRequestMap.remove(consumeRequest);
    }

    public void submit(ConsumeRequest consumeRequest, List<Pair<QueuePair, Integer>> queuePairList, boolean force) {
        Object objLock = consumeRequestLock.fetchLockObject(consumeRequest.getMessageQueueGroup(), consumeRequest.getShardingKeyIndex());

        synchronized (objLock) {
            ConsumeRequest prevRequest = consumeRequestMap.putIfAbsent(consumeRequest, consumeRequest);
            if (prevRequest == null || force) {
                if (prevRequest == null) {
                    consumeRequest.getQueueToConsume().addAll(queuePairList);
                }
                try {
                    threadPoolExecutor.submit(consumeRequest);
                } catch (Exception e) {
                    LOG.error("error submit consume request: {}, mq group: {}, shardingKeyIndex: {}",
                        e.toString(), consumeRequest.getMessageQueueGroup(), consumeRequest.getShardingKeyIndex());
                }
            } else {
                consumeRequestMap.get(consumeRequest).getQueueToConsume().addAll(queuePairList);
            }
        }
    }

    public void submitLater(final ConsumeRequest consumeRequest, final long suspendTimeMillis) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = cs.getDefaultMQPushConsumer().getSuspendCurrentQueueTimeMillis();
        }

        timeMillis = Math.max(10, timeMillis);
        timeMillis = Math.min(30000, timeMillis);

        cs.getScheduledExecutorService().schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeThreadExecutor.this.submit(consumeRequest, null, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    public void tryLockLaterAndConsumeAgain(final ConsumeRequest consumeRequest, final MessageQueue mq, final long delayMills) {
        cs.getScheduledExecutorService().schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = cs.lockOneMessageQueue(mq);
                if (lockOK) {
                    ConsumeThreadExecutor.this.submitLater(consumeRequest, 10);
                } else {
                    ConsumeThreadExecutor.this.submitLater(consumeRequest, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }
}
