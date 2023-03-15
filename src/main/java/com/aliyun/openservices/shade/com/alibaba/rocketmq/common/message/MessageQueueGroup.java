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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

public class MessageQueueGroup {
    private String topic;

    private List<MessageQueue> messageQueueList;

    public MessageQueueGroup(String topic) {
        this.topic = topic;
        this.messageQueueList = new ArrayList<MessageQueue>();
    }

    public MessageQueueGroup(String topic, List<MessageQueue> messageQueueList) {
        this.topic = topic;
        this.messageQueueList = messageQueueList;
    }

    public String getTopic() {
        return topic;
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageQueueGroup group = (MessageQueueGroup) o;
        return Objects.equal(topic, group.topic) &&
            Objects.equal(messageQueueList, group.messageQueueList);
    }

    @Override public int hashCode() {
        return Objects.hashCode(topic, messageQueueList);
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        for (MessageQueue mq : messageQueueList) {
            sb.append(mq.toString());
            sb.append(" ");
        }
        return sb.toString();
    }
}
