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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.stream;

import java.util.List;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.stream.DefaultStream;

public class RegisterBrokerStream extends DefaultStream {
    private final TopicConfigSerializeWrapper topicConfigSerializeWrapper;
    private final List<String> filterServerList;

    public RegisterBrokerStream(
        TopicConfigSerializeWrapper topicConfigSerializeWrapper, List<String> filterServerList) {
        this.topicConfigSerializeWrapper = topicConfigSerializeWrapper;
        this.filterServerList = filterServerList;
        super.serialize();
    }

    public TopicConfigSerializeWrapper getTopicConfigSerializeWrapper() {
        return topicConfigSerializeWrapper;
    }

    public List<String> getFilterServerList() {
        return filterServerList;
    }
}
