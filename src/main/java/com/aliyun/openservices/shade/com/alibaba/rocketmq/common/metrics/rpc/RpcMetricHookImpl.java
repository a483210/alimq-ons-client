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

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics.rpc;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.constant.LoggerName;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics.MetricsInfo;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics.MetricsTimerWrapper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics.MetricsUtils;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.RequestCode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.PopMessageRequestHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLoggerFactory;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.InvocationContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RpcMetricHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.prometheus.client.Histogram;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RpcMetricHookImpl implements RpcMetricHook {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final HashSet<Integer> BROKER_REQUEST_CODE = new HashSet<Integer>();

    private final ConcurrentMap<Integer, MetricsTimerWrapper> latencyTimerMap;

    public RpcMetricHookImpl() {
        this.latencyTimerMap = new ConcurrentHashMap<Integer, MetricsTimerWrapper>();
    }

    static {
        BROKER_REQUEST_CODE.add(RequestCode.PULL_METRIC_DATA);
        BROKER_REQUEST_CODE.add(RequestCode.CHECK_TRANSACTION_STATE);
        BROKER_REQUEST_CODE.add(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED);
        BROKER_REQUEST_CODE.add(RequestCode.RESET_CONSUMER_CLIENT_OFFSET);
        BROKER_REQUEST_CODE.add(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT);
    }

    private TreeMap<String, String> getRpcLabelValues(String remoteAddr, RemotingCommand request) {
        TreeMap<String, String> labelValues = new TreeMap<String, String>();
        for (RpcLabels rpcLabels : RpcLabels.values()) {
            labelValues.put(rpcLabels.getLabel(), "");
        }

        InvocationContext invocationContext = RemotingHelper.THREAD_LOCAL.get();
        if (invocationContext != null) {
            labelValues.putAll(invocationContext.getMetrics());
        }

//        Map<String, String> extFields = request.getExtFields();
//        if (extFields != null) {
//            String accessKey = extFields.get("AccessKey");
//            if (accessKey != null) {
//                labelValues.put(RpcLabels.ACCESS_KEY.getLabel(), accessKey);
//            }
//        }
        String instanceId = request.getNamespaceId();
        if (instanceId != null) {
            labelValues.put(RpcLabels.INSTANCE_ID.getLabel(), instanceId);
        }
        String requestCode = Integer.toString(request.getCode());
        labelValues.put(RpcLabels.REQUEST_CODE.getLabel(), requestCode);
        String topic = "";
        String consumerGroup = "";
        CommandCustomHeader commandCustomHeader = request.readCustomHeader();
        if (commandCustomHeader instanceof SendMessageRequestHeader) {
            topic = ((SendMessageRequestHeader) commandCustomHeader).getTopic();
        } else if (commandCustomHeader instanceof SendMessageRequestHeaderV2) {
            topic = ((SendMessageRequestHeaderV2) commandCustomHeader).getB();
        } else if (commandCustomHeader instanceof PullMessageRequestHeader) {
            PullMessageRequestHeader pullMessageRequestHeader = (PullMessageRequestHeader) commandCustomHeader;
            topic = pullMessageRequestHeader.getTopic();
            consumerGroup = pullMessageRequestHeader.getConsumerGroup();
        } else if (commandCustomHeader instanceof PopMessageRequestHeader) {
            PopMessageRequestHeader popMessageRequestHeader = (PopMessageRequestHeader) commandCustomHeader;
            topic = popMessageRequestHeader.getTopic();
            consumerGroup = popMessageRequestHeader.getConsumerGroup();
        }
        if (topic != null) {
            labelValues.put(RpcLabels.TOPIC.getLabel(), topic);
        }
        if (consumerGroup != null) {
            labelValues.put(RpcLabels.CONSUMER_GROUP.getLabel(), consumerGroup);
        }
        if (remoteAddr != null && remoteAddr.contains(":")) {
            remoteAddr = remoteAddr.split(":")[0];
            labelValues.put(RpcLabels.REMOTE_ADDRESS.getLabel(), remoteAddr);
        }
        return labelValues;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        try {
            TreeMap<String, String> rpcLabelValues = getRpcLabelValues(remoteAddr, request);
            Integer requestCode = Integer.parseInt(rpcLabelValues.get(RpcLabels.REQUEST_CODE.getLabel()));
            if (BROKER_REQUEST_CODE.contains(requestCode)) {
                return;
            }
            MetricsUtils.getCounter(MetricsInfo.TOTAL_RPC_REQUESTS.getName(), rpcLabelValues,
                MetricsInfo.TOTAL_RPC_REQUESTS.getHelp()).inc();
            if (!request.isOnewayRPC()) {
                MetricsUtils.getGauge(MetricsInfo.IN_FLIGHT_RPC_REQUESTS.getName(), rpcLabelValues,
                    MetricsInfo.IN_FLIGHT_RPC_REQUESTS.getHelp()).inc();
                Histogram.Timer timer =
                    MetricsUtils.getHistogram(MetricsInfo.RPC_REQUESTS_LATENCY.getName(), rpcLabelValues,
                        MetricsInfo.RPC_REQUESTS_LATENCY.getHelp()).startTimer();
                int opaque = request.getOpaque();
                MetricsTimerWrapper metricsTimerWrapper = new MetricsTimerWrapper(timer);
                latencyTimerMap.put(opaque, metricsTimerWrapper);
            }
        } catch (Exception e) {
            LOGGER.error("Error occurs when executing hook for rpc metrics", e);
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        try {
            TreeMap<String, String> rpcLabelValues = getRpcLabelValues(remoteAddr, request);
            Integer requestCode = Integer.parseInt(rpcLabelValues.get(RpcLabels.REQUEST_CODE.getLabel()));
            if (BROKER_REQUEST_CODE.contains(requestCode)) {
                return;
            }
            MetricsUtils.getGauge(MetricsInfo.IN_FLIGHT_RPC_REQUESTS.getName(), rpcLabelValues,
                MetricsInfo.IN_FLIGHT_RPC_REQUESTS.getHelp()).dec();
            rpcLabelValues.put(RpcLabels.RESPONSE_CODE.getLabel(), Integer.toString(response.getCode()));
            MetricsUtils.getCounter(MetricsInfo.TOTAL_RPC_RESPONSES.getName(), rpcLabelValues,
                MetricsInfo.TOTAL_RPC_RESPONSES.getHelp()).inc();
            int opaque = request.getOpaque();
            MetricsTimerWrapper metricsTimerWrapper = latencyTimerMap.remove(opaque);
            if (metricsTimerWrapper != null) {
                Histogram.Timer timer = metricsTimerWrapper.getTimer();
                timer.observeDuration();
            }
        } catch (Exception e) {
            LOGGER.error("Error occurs when executing hook for rpc metrics", e);
        } finally {
            RemotingHelper.removeThreadLocal();
        }
    }

    @Override
    public void doAfterRpcFailure(String remoteAddr, RemotingCommand request, Boolean remoteTimeout) {
        try {
            TreeMap<String, String> rpcLabelValues = getRpcLabelValues(remoteAddr, request);
            Integer requestCode = Integer.parseInt(rpcLabelValues.get(RpcLabels.REQUEST_CODE.getLabel()));
            if (BROKER_REQUEST_CODE.contains(requestCode)) {
                return;
            }
            int opaque = request.getOpaque();
            MetricsTimerWrapper metricsTimerWrapper = latencyTimerMap.remove(opaque);
            if (metricsTimerWrapper != null) {
                Histogram.Timer timer = metricsTimerWrapper.getTimer();
                timer.observeDuration();
            }
            if (remoteTimeout) {
                MetricsUtils.getCounter(MetricsInfo.TIMEOUT_RPC_REQUESTS.getName(), rpcLabelValues,
                    MetricsInfo.TIMEOUT_RPC_REQUESTS.getHelp()).inc();
            } else {
                MetricsUtils.getCounter(MetricsInfo.ERROR_RPC_REQUESTS.getName(), rpcLabelValues,
                    MetricsInfo.ERROR_RPC_REQUESTS.getHelp()).inc();
            }
            if (!request.isOnewayRPC()) {
                MetricsUtils.getGauge(MetricsInfo.IN_FLIGHT_RPC_REQUESTS.getName(), rpcLabelValues,
                    MetricsInfo.IN_FLIGHT_RPC_REQUESTS.getHelp()).dec();
            }
        } catch (Exception e) {
            LOGGER.error("Error occurs when executing hook for rpc metrics", e);
        } finally {
            RemotingHelper.removeThreadLocal();
        }
    }

    @Override
    public int cleanExpiredTimer() {
        int expiredTimerNum = 0;
        Iterator<Entry<Integer, MetricsTimerWrapper>> iterator = latencyTimerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getValue().isExpired()) {
                iterator.remove();
                expiredTimerNum++;
            }
        }
        return expiredTimerNum;
    }
}
