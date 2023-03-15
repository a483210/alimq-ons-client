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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics;

public enum MetricsInfo {
    // For rpc
    TOTAL_RPC_REQUESTS("total_rpc_requests", "total rpc requests"),
    TOTAL_RPC_RESPONSES("total_rpc_responses", "total rpc responses"),
    ERROR_RPC_REQUESTS("error_rpc_requests", "error rpc requests"),
    TIMEOUT_RPC_REQUESTS("timeout_rpc_requests", "timeout rpc requests"),
    IN_FLIGHT_RPC_REQUESTS("in_flight_rpc_requests", "in-flight rpc requests"),
    RPC_REQUESTS_LATENCY("rpc_requests_latency", "rpc requests latency");
    // For others

    private final String name;
    private final String help;

    MetricsInfo(String name, String help) {
        this.name = name;
        this.help = help;
    }

    public String getName() {
        return name;
    }

    public String getHelp() {
        return help;
    }
}
