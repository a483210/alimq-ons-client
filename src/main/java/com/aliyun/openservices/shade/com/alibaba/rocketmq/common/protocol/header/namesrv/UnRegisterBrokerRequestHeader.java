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
 * $Id: UnRegisterBrokerRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class UnRegisterBrokerRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String brokerName;
    @CFNotNull
    private String brokerAddr;
    @CFNotNull
    private String clusterName;
    @CFNotNull
    private Long brokerId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
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

    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final UnRegisterBrokerRequestHeader header = (UnRegisterBrokerRequestHeader) o;
        return Objects.equal(brokerName, header.brokerName) &&
            Objects.equal(brokerAddr, header.brokerAddr) &&
            Objects.equal(clusterName, header.clusterName) &&
            Objects.equal(brokerId, header.brokerId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(brokerName, brokerAddr, clusterName, brokerId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("brokerName", brokerName)
            .add("brokerAddr", brokerAddr)
            .add("clusterName", clusterName)
            .add("brokerId", brokerId)
            .toString();
    }
}
