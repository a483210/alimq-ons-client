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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

public class ProcessQueueGroup {

    public enum ProcessQueueGroupStatus {
        // all pqs in group are dropped.
        ALL_DROPPED,
        // part of pqs are dropped.
        PARTIALLY_DROPPED,
        // no pq is dropped.
        NOT_DROPPED
    }

    private List<ProcessQueue> processQueueList;

    public ProcessQueueGroup() {
        this.processQueueList = new ArrayList<ProcessQueue>();
    }

    public ProcessQueueGroup(List<ProcessQueue> processQueueList) {
        this.processQueueList = processQueueList;
    }

    public List<ProcessQueue> getProcessQueueList() {
        return processQueueList;
    }

    public ProcessQueueGroupStatus getProcessQueueStatus() {
        int droppedCnt = 0;
        for (ProcessQueue pq : processQueueList) {
            if (pq.isDropped()) {
                droppedCnt++;
            }
        }

        if (droppedCnt == 0) {
            return ProcessQueueGroupStatus.NOT_DROPPED;
        }
        if (droppedCnt == processQueueList.size()) {
            return ProcessQueueGroupStatus.ALL_DROPPED;
        } else {
            return ProcessQueueGroupStatus.PARTIALLY_DROPPED;
        }
    }

    public boolean isLocked() {
        for (ProcessQueue pq : processQueueList) {
            if (!pq.isLocked()) {
                return false;
            }
        }
        return true;
    }

    public boolean isLockExpired() {
        for (ProcessQueue pq : processQueueList) {
            if (pq.isLockExpired()) {
                return true;
            }
        }
        return false;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessQueueGroup that = (ProcessQueueGroup) o;
        return Objects.equal(processQueueList, that.processQueueList);
    }

    @Override public int hashCode() {
        return Objects.hashCode(processQueueList);
    }
}
