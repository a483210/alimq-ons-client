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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.admin;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

public class ConsumeStats extends RemotingSerializable {

    /**
     * TODO: Normally, we should have created a second map to store accumulation estimation per message queue.
     * Unfortunately, this would trigger bug of FastJSON.
     */
    private Map<MessageQueue, OffsetWrapper> offsetTable = new ConcurrentHashMap<MessageQueue, OffsetWrapper>();

    private double consumeTps = 0;

    public long computeTotalDiff() {
        long diffTotal = 0L;
        for (Entry<MessageQueue, OffsetWrapper> next : this.offsetTable.entrySet()) {
            if (next.getValue().getEstimatedAccumulation() >= 0) {
                diffTotal += next.getValue().getEstimatedAccumulation();
            } else {
                diffTotal += next.getValue().getBrokerOffset() - next.getValue().getConsumerOffset();
            }
        }
        return diffTotal;
    }

    public Map<MessageQueue, OffsetWrapper> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(Map<MessageQueue, OffsetWrapper> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public double getConsumeTps() {
        return consumeTps;
    }

    public void setConsumeTps(double consumeTps) {
        this.consumeTps = consumeTps;
    }
}
