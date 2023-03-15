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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.batch;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;

public class TopicCache {

    private static final InternalLogger LOGGER = ClientLogger.getLog();

    /**
     * Topic in perspective of end user, aka, topic name with namespace and retry prefix stripped.
     */
    private final String topic;

    /**
     * Messages to dispatch cache. Note priority block queue permits duplicate elements and this feature is essential
     * since we may have messages with the same queue offset from different full-qualified-named topics considering
     * retry topics and topics with/without namespace.
     */
    private final ConcurrentMap<MessageQueue, PriorityBlockingQueue<MessageExt>> cache;

    public TopicCache(String topic) {
        this.topic = topic;
        this.cache = new ConcurrentHashMap<MessageQueue, PriorityBlockingQueue<MessageExt>>();
    }

    public int size() {
        int total = 0;
        for (Map.Entry<MessageQueue, PriorityBlockingQueue<MessageExt>> entry : cache.entrySet()) {
            total += entry.getValue().size();
        }
        return total;
    }

    /**
     * Note duplicate messages are also accepted since we are using heap data structure.
     *
     * @param messages messages to add.
     */
    public void put(Collection<MessageExt> messages) {
        for (MessageExt message : messages) {
            if (!topic.equals(message.getTopic())) {
                LOGGER.warn("Trying to put an message with mismatched topic name, expect: {}, actual: {}, actualFQN: " +
                        "{}", topic, message.getTopic(), ConsumeMessageConcurrentlyService.topicOf(message));
                continue;
            }
            // legitimate message queue
            MessageQueue messageQueue = new MessageQueue(ConsumeMessageConcurrentlyService.topicOf(message),
                message.getBrokerName(), message.getQueueId());
            if (!cache.containsKey(messageQueue)) {
                PriorityBlockingQueue<MessageExt> cacheItem = new PriorityBlockingQueue<MessageExt>(128, new Comparator<MessageExt>() {
                    @Override
                    public int compare(MessageExt o1, MessageExt o2) {
                        long lhs = o1.getQueueOffset();
                        long rhs = o2.getQueueOffset();
                        return lhs < rhs ? -1 : (lhs == rhs ? 0 : 1);
                    }
                });
                cache.putIfAbsent(messageQueue, cacheItem);
            }
            cache.get(messageQueue).add(message);
        }
    }

    /**
     * Take <code>count</code> messages out of heap cache.
     *
     * @param count
     * @param collection
     * @return
     */
    public boolean take(final int count, Collection<MessageExt> collection) {
        if (size() < count) {
            return false;
        }
        int remain = count;
        while (remain > 0) {
            MessageExt candidate = null;
            PriorityBlockingQueue<MessageExt> targetQueue = null;
            for (Map.Entry<MessageQueue, PriorityBlockingQueue<MessageExt>> entry : cache.entrySet()) {
                PriorityBlockingQueue<MessageExt> messages = entry.getValue();
                if (null == candidate) {
                    candidate = messages.peek();
                    targetQueue = messages;
                } else {
                    MessageExt challenger = messages.peek();
                    if (null == challenger) {
                        continue;
                    }

                    if (challenger.getDecodedTime() < candidate.getDecodedTime()) {
                        candidate = challenger;
                        targetQueue = messages;
                    }
                }
            }

            if (null != candidate) {
                collection.add(targetQueue.poll());
                --remain;
            }
        }
        return true;
    }

    /**
     * Time elapsed since the first message in the heap cache pulled from broker(to be exact, since it is locally
     * decoded).
     *
     * @return
     */
    public long elapsed() {
        long earliest = System.currentTimeMillis();
        long current = earliest;
        for (Map.Entry<MessageQueue, PriorityBlockingQueue<MessageExt>> entry : cache.entrySet()) {
            MessageExt message = entry.getValue().peek();
            if (null != message) {
                long decodedTime = message.getDecodedTime();
                if (decodedTime < earliest) {
                    earliest = decodedTime;
                }
            }
        }
        return current - earliest;
    }

}
