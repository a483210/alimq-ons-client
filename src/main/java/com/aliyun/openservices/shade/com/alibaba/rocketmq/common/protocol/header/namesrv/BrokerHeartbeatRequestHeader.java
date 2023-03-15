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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingCommandException;

public class BrokerHeartbeatRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String clusterName;
    @CFNotNull
    private String brokerAddr;
    @CFNotNull
    private String brokerName;

    @Override
    public void checkFields() throws RemotingCommandException {
        if (clusterName == null) {
            throw new RemotingCommandException("BrokerHeartbeatRequest exception, clusterName is null");
        }

        if (brokerAddr == null) {
            throw new RemotingCommandException("BrokerHeartbeatRequest exception, brokerAddr is null");
        }

        if (brokerName == null) {
            throw new RemotingCommandException("BrokerHeartbeatRequest exception, brokerName is null");
        }
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
