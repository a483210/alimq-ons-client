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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingSysResponseCode;

public class ResponseCode extends RemotingSysResponseCode {

    public static final int FLUSH_DISK_TIMEOUT = 10;

    public static final int SLAVE_NOT_AVAILABLE = 11;

    public static final int FLUSH_SLAVE_TIMEOUT = 12;

    public static final int MESSAGE_ILLEGAL = 13;

    public static final int SERVICE_NOT_AVAILABLE = 14;

    public static final int VERSION_NOT_SUPPORTED = 15;

    public static final int NO_PERMISSION = 16;

    public static final int TOPIC_NOT_EXIST = 17;
    public static final int TOPIC_EXIST_ALREADY = 18;
    public static final int PULL_NOT_FOUND = 19;

    public static final int PULL_RETRY_IMMEDIATELY = 20;

    public static final int PULL_OFFSET_MOVED = 21;

    public static final int QUERY_NOT_FOUND = 22;

    public static final int SUBSCRIPTION_PARSE_FAILED = 23;

    public static final int SUBSCRIPTION_NOT_EXIST = 24;

    public static final int SUBSCRIPTION_NOT_LATEST = 25;

    public static final int SUBSCRIPTION_GROUP_NOT_EXIST = 26;

    public static final int FILTER_DATA_NOT_EXIST = 27;

    public static final int FILTER_DATA_NOT_LATEST = 28;

    public static final int TRANSACTION_SHOULD_COMMIT = 200;

    public static final int TRANSACTION_SHOULD_ROLLBACK = 201;

    public static final int TRANSACTION_STATE_UNKNOW = 202;

    public static final int TRANSACTION_STATE_GROUP_WRONG = 203;
    public static final int NO_BUYER_ID = 204;

    public static final int NOT_IN_CURRENT_UNIT = 205;

    public static final int CONSUMER_NOT_ONLINE = 206;

    public static final int CONSUME_MSG_TIMEOUT = 207;

    public static final int NO_MESSAGE = 208;
    
    public static final int POLLING_FULL = 209;

    public static final int POLLING_TIMEOUT = 210;

    /**
     * SDK user tries to configure an illegal value for the configurable item.
     */
    public static final int BAD_CONFIGURATION = 600;

    /**
     * Error code when trying to reset consume offset to a value less than min of consume queue.
     */
    public static final int OFFSET_TOO_SMALL = 601;

    /**
     * Error code when trying to reset consume offset to a value greater than max of consume queue.
     */
    public static final int OFFSET_TOO_LARGE = 602;


    public static final int OPERATION_NOT_SUPPORTED_BY_SLAVE = 603;

    public static final int ILLEGAL_OPERATION = 604;

}
