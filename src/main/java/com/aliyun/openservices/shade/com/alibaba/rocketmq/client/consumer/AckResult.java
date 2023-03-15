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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer;

public class AckResult {
    private int responseCode;
    private String responseRemark;
    private AckStatus status;
    private String extraInfo;
    private long popTime;

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public String getResponseRemark() {
        return responseRemark;
    }

    public void setResponseRemark(String responseRemark) {
        this.responseRemark = responseRemark;
    }

    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }

    public long getPopTime() {
        return popTime;
    }

    public AckStatus getStatus() {
        return status;
    }

    public void setStatus(AckStatus status) {
        this.status = status;
    }

    public void setExtraInfo(String extraInfo) {
        this.extraInfo = extraInfo;
    }

    public String getExtraInfo() {
        return extraInfo;
    }

    @Override
    public String toString() {
        return "AckResult [" +
            "responseCode=" + responseCode +
            ", responseRemark='" + responseRemark + '\'' +
            ", status=" + status +
            ", extraInfo='" + extraInfo + '\'' +
            ", popTime=" + popTime +
            ']';
    }
}
