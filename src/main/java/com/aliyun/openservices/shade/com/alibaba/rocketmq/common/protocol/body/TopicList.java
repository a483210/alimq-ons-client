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

import com.alibaba.ons.open.trace.core.utils.JsonUtils;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.ByteBufferInputStream;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class TopicList extends RemotingSerializable {
    private Set<String> topicList = new CopyOnWriteArraySet<String>();
    private String brokerAddr;

    public Set<String> getTopicList() {
        return topicList;
    }

    public void setTopicList(Set<String> topicList) {
        this.topicList = topicList;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public static TopicList decode(final ByteBuffer data) {
        TopicList list = new TopicList();
        Set<String> tmpList = new HashSet<String>();
        InputStream inputStream = new ByteBufferInputStream(data);

        JsonNode jsonNode = JsonUtils.readTree(new InputStreamReader(inputStream));

        jsonNode.fields().forEachRemaining(field -> {
            String key = field.getKey();
            JsonNode value = field.getValue();

            if ("brokerAddr".equals(key)) {
                list.setBrokerAddr(value.asText());
            } else if ("topicList".equals(key) && value.isArray()) {
                ArrayNode arrayNode = ((ArrayNode) value);
                arrayNode.forEach(node -> {
                    tmpList.add(node.asText());
                });
                list.getTopicList().addAll(tmpList);
            }
        });
        return list;
    }
}
