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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueAveragely<MessageQueue> {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll, log)) {
            return result;
        }

        Map<Integer, MessageQueue> mainQueueMap = new HashMap<Integer, MessageQueue>();

        for (MessageQueue mq : mqAll) {
            if (mq.getQueueGroupId() != MessageQueue.NO_QUEUE_GROUP && !mainQueueMap.containsKey(mq.getQueueGroupId())) {
                mainQueueMap.put(mq.getQueueGroupId(), mq);
            }
        }

        Map<MessageQueue, List<MessageQueue>> queueGroupMap = new HashMap<MessageQueue, List<MessageQueue>>();
        for (MessageQueue mq : mqAll) {
            int queueGroupId = mq.getQueueGroupId();
            MessageQueue mqKey;
            if (queueGroupId == -1) {
                mqKey = mq;
            } else {
                mqKey = mainQueueMap.get(mq.getQueueGroupId());
            }
            if (!queueGroupMap.containsKey(mqKey)) {
                queueGroupMap.put(mqKey, new ArrayList<MessageQueue>());
            }
            queueGroupMap.get(mqKey).add(mq);
        }

        List<MessageQueue> mqToAllocate = new ArrayList<MessageQueue>(queueGroupMap.keySet());
        Collections.sort(mqToAllocate);
        List<MessageQueue> allocateResult = allocateAveragely(currentCID, cidAll, mqToAllocate);

        for (MessageQueue mq : allocateResult) {
            result.addAll(queueGroupMap.get(mq));
        }

        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
