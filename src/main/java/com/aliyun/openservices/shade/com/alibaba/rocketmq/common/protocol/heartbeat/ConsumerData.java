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
 * $Id: ConsumerData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class ConsumerData {
    private String groupName;
    private ConsumeType consumeType;
    private MessageModel messageModel;
    private ConsumeFromWhere consumeFromWhere;
    private Set<SubscriptionData> subscriptionDataSet = new HashSet<SubscriptionData>();
    private boolean unitMode;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public Set<SubscriptionData> getSubscriptionDataSet() {
        return subscriptionDataSet;
    }

    public void setSubscriptionDataSet(Set<SubscriptionData> subscriptionDataSet) {
        this.subscriptionDataSet = subscriptionDataSet;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    @Override
    public String toString() {
        return "ConsumerData [groupName=" + groupName + ", consumeType=" + consumeType + ", messageModel="
            + messageModel + ", consumeFromWhere=" + consumeFromWhere + ", unitMode=" + unitMode
            + ", subscriptionDataSet=" + subscriptionDataSet + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConsumerData that = (ConsumerData) o;

        if (unitMode != that.unitMode) {
            return false;
        }
        if (groupName != null ? !groupName.equals(that.groupName) : that.groupName != null) {
            return false;
        }
        if (consumeType != that.consumeType) {
            return false;
        }
        if (messageModel != that.messageModel) {
            return false;
        }
        if (consumeFromWhere != that.consumeFromWhere) {
            return false;
        }
        return subscriptionDataSet != null ? subscriptionDataSet.equals(that.subscriptionDataSet) :
                that.subscriptionDataSet == null;
    }

    @Override
    public int hashCode() {
        int result = groupName != null ? groupName.hashCode() : 0;
        result = 31 * result + (consumeType != null ? consumeType.hashCode() : 0);
        result = 31 * result + (messageModel != null ? messageModel.hashCode() : 0);
        result = 31 * result + (consumeFromWhere != null ? consumeFromWhere.hashCode() : 0);
        result = 31 * result + (subscriptionDataSet != null ? subscriptionDataSet.hashCode() : 0);
        result = 31 * result + (unitMode ? 1 : 0);
        return result;
    }
}
