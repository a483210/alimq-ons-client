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

/**
 * Message selector: select message at server.
 * <p>
 * Now, support:
 * <li>Tag: {@link com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType#TAG}
 * </li>
 * <li>SQL92: {@link com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType#SQL92}
 * </li>
 * </p>
 */
public class MessageSelector {

    /**
     * @see com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType
     */
    private String type;

    /**
     * expression content.
     */
    private String expression;
    /**
     * self define properties, just an extend point.
     */
    private Map<String, String> properties = new HashMap<String, String>(4);

    private MessageSelector(String type, String expression) {
        this.type = type;
        this.expression = expression;
    }

    /**
     * Use SLQ92 to select message.
     *
     * @param sql if null or empty, will be treated as select all message.
     */
    public static MessageSelector bySql(String sql) {
        return new MessageSelector(ExpressionType.SQL92, sql);
    }

    /**
     * Use tag to select message.
     *
     * @param tag if null or empty or "*", will be treated as select all message.
     */
    public static MessageSelector byTag(String tag) {
        return new MessageSelector(ExpressionType.TAG, tag);
    }

    public String getExpressionType() {
        return type;
    }

    public String getExpression() {
        return expression;
    }

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
}
