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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.CheckSendBackHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;

/**
 * Consumer concurrent consumption context
 */
public abstract class ConsumeConcurrentlyContext {
    /**
     * Message consume retry strategy<br>
     * -1,no retry,put into DLQ directly<br>
     * 0,broker control retry frequency<br>
     * >0,client control retry frequency
     */
    protected int delayLevelWhenNextConsume = 0;
    protected int ackIndex = Integer.MAX_VALUE;
    private CheckSendBackHook checkSendBackHook;
    private ConsumeExactlyOnceStatus exactlyOnceStatus = ConsumeExactlyOnceStatus.NO_EXACTLYONCE;

    public ConsumeConcurrentlyContext() {
    }

    public int getDelayLevelWhenNextConsume() {
        return delayLevelWhenNextConsume;
    }

    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }

    public abstract MessageQueue getMessageQueue();

    public int getAckIndex() {
        return ackIndex;
    }

    public void setAckIndex(int ackIndex) {
        this.ackIndex = ackIndex;
    }

    public CheckSendBackHook getCheckSendBackHook() {
        return checkSendBackHook;
    }

    public void setCheckSendBackHook(CheckSendBackHook checkSendBackHook) {
        this.checkSendBackHook = checkSendBackHook;
    }

    public ConsumeExactlyOnceStatus getExactlyOnceStatus() {
        return exactlyOnceStatus;
    }

    public void setExactlyOnceStatus(ConsumeExactlyOnceStatus exactlyOnceStatus) {
        this.exactlyOnceStatus = exactlyOnceStatus;
    }
}
