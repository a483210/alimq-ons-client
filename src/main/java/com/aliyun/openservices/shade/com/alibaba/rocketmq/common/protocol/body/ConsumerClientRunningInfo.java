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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @ClassName ConsumerQueueRunningInfo
 * @Author xuanchu.wjj
 * @Date 2019/10/16 3:52 PM
 */
public class ConsumerClientRunningInfo extends RemotingSerializable {

    private TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>> clientMqTable = new TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>>();


    public TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>> getClientMqTable() {
        return clientMqTable;
    }

    public String formatStringTopicClientQueueStat() {
        TreeMap<String, TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>>> topicClientQueue = new TreeMap<String, TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>>>();

        Iterator<Map.Entry<String, TreeMap<MessageQueue, ProcessQueueInfo>>> it = this.clientMqTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, TreeMap<MessageQueue, ProcessQueueInfo>> next = it.next();
            if (next.getValue() == null) {
                continue;
            }
            String clientId = next.getKey();
            TreeMap<MessageQueue, ProcessQueueInfo> mqTable = next.getValue();
            Iterator<Map.Entry<MessageQueue, ProcessQueueInfo>> itMqTable = mqTable.entrySet().iterator();
            while (itMqTable.hasNext()) {
                Map.Entry<MessageQueue, ProcessQueueInfo> nextMqTable = itMqTable.next();
                if (nextMqTable.getKey() == null) {
                    continue;
                }
                TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>> clientQueue = topicClientQueue.get(nextMqTable.getKey().getTopic());
                if (clientQueue == null) {
                    clientQueue = new TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>>();
                    topicClientQueue.put(nextMqTable.getKey().getTopic(), clientQueue);
                }
                TreeMap<MessageQueue, ProcessQueueInfo> queueMap = clientQueue.get(clientId);
                if (queueMap == null) {
                    queueMap = new TreeMap<MessageQueue, ProcessQueueInfo>();
                    clientQueue.put(clientId, queueMap);
                }
                queueMap.put(nextMqTable.getKey(), nextMqTable.getValue());
            }
        }

        StringBuilder sb = new StringBuilder();

        sb.append("\n\n#Consumer Client Running Info#\n");
        sb.append(String.format("%-64s  %-64s  %-64s  %-4s%n",
            "#Topic",
            "#ConsumerClientID",
            "#Broker Name",
            "#QID"
        ));

        Iterator<Map.Entry<String, TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>>>> itTopicClient = topicClientQueue.entrySet().iterator();
        while (itTopicClient.hasNext()) {
            Map.Entry<String, TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>>> next = itTopicClient.next();
            if (next.getValue() == null) {
                continue;
            }
            String topic = next.getKey();

            TreeMap<String, TreeMap<MessageQueue, ProcessQueueInfo>> clientQueueMap = next.getValue();
            Iterator<Map.Entry<String, TreeMap<MessageQueue, ProcessQueueInfo>>> itClientQueue = clientQueueMap.entrySet().iterator();
            while (itClientQueue.hasNext()) {
                Map.Entry<String, TreeMap<MessageQueue, ProcessQueueInfo>> nextClientQueue = itClientQueue.next();
                if (nextClientQueue.getValue() == null) {
                    continue;
                }
                String clientId = nextClientQueue.getKey();

                TreeMap<MessageQueue, ProcessQueueInfo> queueMap = nextClientQueue.getValue();
                Iterator<Map.Entry<MessageQueue, ProcessQueueInfo>> itQueue = queueMap.entrySet().iterator();
                while (itQueue.hasNext()) {
                    Map.Entry<MessageQueue, ProcessQueueInfo> queueMapEntry = itQueue.next();
                    if (queueMapEntry.getKey() == null) {
                        continue;
                    }
                    String item = String.format("%-64s  %-64s  %-64s  %-4d%n",
                        topic,
                        clientId,
                        queueMapEntry.getKey().getBrokerName(),
                        queueMapEntry.getKey().getQueueId());

                    sb.append(item);
                }
            }
        }

        return sb.toString();
    }

}
