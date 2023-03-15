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
 * $Id: QueueData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

public class QueueData implements Comparable<QueueData> {
    private String brokerName;
    private int readQueueNums;
    private int writeQueueNums;
    private int perm;
    private int topicSynFlag;

    private List<QueueDesc> queueDescList;

    public QueueData() {
    }

    // Deep copy QueueData
    public QueueData(QueueData queueData) {
        this.brokerName = queueData.brokerName;
        this.readQueueNums = queueData.readQueueNums;
        this.writeQueueNums = queueData.writeQueueNums;
        this.perm = queueData.perm;
        this.topicSynFlag = queueData.topicSynFlag;

        if (queueData.queueDescList != null) {
            this.queueDescList = new ArrayList<QueueDesc>();
            for (final QueueDesc desc : queueData.queueDescList) {
                this.queueDescList.add(new QueueDesc(desc));
            }
        }
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSynFlag() {
        return topicSynFlag;
    }

    public void setTopicSynFlag(int topicSynFlag) {
        this.topicSynFlag = topicSynFlag;
    }

    public List<QueueDesc> getQueueDescList() {
        return queueDescList;
    }

    public void setQueueDescList(List<QueueDesc> queueDescList) {
        this.queueDescList = queueDescList;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSynFlag;
        if (queueDescList != null) {
            result = prime * result + queueDescList.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (perm != other.perm)
            return false;
        if (readQueueNums != other.readQueueNums)
            return false;
        if (writeQueueNums != other.writeQueueNums)
            return false;
        if (topicSynFlag != other.topicSynFlag)
            return false;
        if (queueDescList == null) {
            if (other.queueDescList != null) {
                return false;
            }
        } else if (!queueDescList.equals(other.queueDescList))
            return false;
        return true;
    }

    @Override public String toString() {
        return new ToStringBuilder(this)
            .append("brokerName", brokerName)
            .append("readQueueNums", readQueueNums)
            .append("writeQueueNums", writeQueueNums)
            .append("perm", perm)
            .append("topicSynFlag", topicSynFlag)
            .append("queueDescList", queueDescList)
            .toString();
    }

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
