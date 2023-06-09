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

/**
 * $Id: GetMaxOffsetRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingCommandException;

public class GetMaxOffsetRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private Integer queueId;

    /**
     * A message at committed offset has been dispatched from Topic to MessageQueue, so it can be consumed immediately,
     * while a message at inflight offset is not visible for a consumer temporarily.
     * Set this flag true if the max committed offset is needed, or false if the max inflight offset is preferred.
     * The default value is true.
     */
    @CFNullable
    private boolean committed = true;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(final boolean committed) {
        this.committed = committed;
    }
}
