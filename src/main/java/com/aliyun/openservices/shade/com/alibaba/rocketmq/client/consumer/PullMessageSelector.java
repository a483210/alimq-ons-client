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

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageDecoder;

import java.util.HashMap;
import java.util.Map;

public class PullMessageSelector {


    private String type;
    private String expression;

    protected PullMessageSelector(String expression) {
        this.expression = expression;
    }

    protected PullMessageSelector(String type, String expression) {
        this.type = type;
        this.expression = expression;
    }

    public static PullMessageSelector byTag(String tag) {
        return new PullMessageSelector(ExpressionType.TAG, tag);
    }

    public static PullMessageSelector bySql(String expression) {
        return new PullMessageSelector(ExpressionType.SQL92, expression);
    }

    public static PullMessageSelector all() {
        return new PullMessageSelector(ExpressionType.TAG, "*");
    }

    private long offset = -1;
    private int maxNums = 0;
    private long timeout = 0;
    private boolean blockIfNotFound = false;

    /**
     * self define properties, just an extend point.
     */
    private Map<String, String> properties = new HashMap<String, String>(4);

    public void putProperty(String key, String value) {
        if (key == null || value == null || key.trim() == "" || value.trim() == "") {
            throw new IllegalArgumentException(
                "Key and Value can not be null or empty string!"
            );
        }
        this.properties.put(key, value);
    }

    public void putAllProperties(Map<String, String> puts) {
        if (puts == null || puts.isEmpty()) {
            return;
        }
        this.properties.putAll(puts);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getPropertiesStr() {
        if (this.properties == null || this.properties.isEmpty()) {
            return null;
        }
        return MessageDecoder.messageProperties2String(this.properties);
    }

    /**
     * from where to pull
     *
     * @param offset
     * @return
     */
    public PullMessageSelector from(long offset) {
        this.offset = offset;
        return this;
    }

    /**
     * max pulling numbers
     *
     * @param maxNums
     * @return
     */
    public PullMessageSelector count(int maxNums) {
        this.maxNums = maxNums;
        return this;
    }

    /**
     * timeout
     *
     * @param timeout
     * @return
     */
    public PullMessageSelector timeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * if no message arrival,whether blocking.
     *
     * @param block
     * @return
     */
    public PullMessageSelector blockIfNotFound(boolean block) {
        this.blockIfNotFound = block;
        return this;
    }

    public String getType() {
        return type;
    }

    public String getExpression() {
        return expression;
    }

    public long getOffset() {
        return offset;
    }

    public int getMaxNums() {
        return maxNums;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isBlockIfNotFound() {
        return blockIfNotFound;
    }
}
