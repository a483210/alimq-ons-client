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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client;

import java.util.Map;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.TlsSystemConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.LanguageCode;
import org.apache.commons.lang3.StringUtils;

/**
 * Client Common configuration
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";
    public static final String SOCKS_PROXY_JSON = "com.rocketmq.socksProxyJson";
    public static final String DECODE_READ_BODY = "com.rocketmq.read.body";
    public static final String DECODE_DECOMPRESS_BODY = "com.rocketmq.decompress.body";
    public static final String AUTO_CLEAN_NO_ROUTE_TOPIC = "com.rocketmq.autoclean.noroute.topic";
    public static final String CLIENT_CALLBACK_EXECUTOR_THREAD_NUMS = "client.callback.executor.thread.nums";
    public static final String FETCH_REMOTE_CLIENT_CONFIG = "com.rocketmq.client.config.fetchRemote";
    public static final String SEND_LATENCY_ENABLE = "com.rocketmq.sendLatencyEnable";
    public static final String START_DETECTOR_ENABLE = "com.rocketmq.startDetectorEnable";

    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    private String clientIP = RemotingUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    protected String namespace;

    private int clientCallbackExecutorThreads = Integer.parseInt(System.getProperty(CLIENT_CALLBACK_EXECUTOR_THREAD_NUMS,
        String.valueOf(Runtime.getRuntime().availableProcessors())));
    /**
     * Pulling topic information interval from the named server
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * Update consume queue offset interval from broker
     */
    private int updateConsumeQueueOffsetInterval = 1000 * 5;
    /**
     * Heartbeat interval in microseconds with message broker
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    /**
     * Whether fetch remote client config from nameserver.
     */
    private boolean fetchRemoteClientConfigEnable = Boolean.parseBoolean(System.getProperty(FETCH_REMOTE_CLIENT_CONFIG, "false"));
    /**
     * Update clients' config from namesrv's interval
     */
    private int clientConfigInterval = 1000 * 60;
    /**
     * Detect the Broker's states' interval
     */
    private int detectBrokerInterval = 1000 * 5;
    /**
     * Config about latency strategy.
     */
    private int detectTimeout = 200;
    private int detectInterval = 2000;

    private long pullTimeDelayMillsWhenException = 1000;

    private boolean unitMode = false;
    private boolean allClientsOffline = false;
    private String unitName;
    private Map<String, String> unitPara;
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "true"));
    private boolean decodeReadBody = Boolean.parseBoolean(System.getProperty(DECODE_READ_BODY, "true"));
    private boolean decodeDecompressBody = Boolean.parseBoolean(System.getProperty(DECODE_DECOMPRESS_BODY, "true"));
    private boolean autoCleanTopicRouteNotFound = Boolean.parseBoolean(System.getProperty(AUTO_CLEAN_NO_ROUTE_TOPIC, "false"));
    private boolean recordApiStats = false;
    private boolean disableCallbackExecutor = false;
    private boolean disableNettyWorkerGroup = false;
    private boolean sendLatencyEnable = Boolean.parseBoolean(System.getProperty(SEND_LATENCY_ENABLE, "false"));
    private boolean startDetectorEnable = Boolean.parseBoolean(System.getProperty(START_DETECTOR_ENABLE, "false"));


    private boolean useTLS = TlsSystemConfig.tlsEnable;

    private String sockProxyJson = System.getProperty(SOCKS_PROXY_JSON, "{}");

    private LanguageCode language = LanguageCode.JAVA;

    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        return sb.toString();
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void changeInstanceNameToPID() {
        if ("DEFAULT".equals(this.instanceName)) {
            this.instanceName = String.valueOf(UtilAll.getPid());
        }
    }

    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<String>();
        for (String resource : resourceSet) {
            resourceWithNamespace.add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    public String withoutNamespace(String resource) {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<String>();
        for (String resource : resourceSet) {
            resourceWithoutNamespace.add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    public MessageQueue queueWithNamespace(MessageQueue queue) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queue;
        }

        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId(),
            queue.getQueueGroupId(), queue.isMainQueue());
    }

    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        Iterator<MessageQueue> iter = queues.iterator();
        while (iter.hasNext()) {
            MessageQueue queue = iter.next();
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.updateConsumeQueueOffsetInterval = cc.updateConsumeQueueOffsetInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.clientConfigInterval = cc.clientConfigInterval;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.decodeReadBody = cc.decodeReadBody;
        this.decodeDecompressBody = cc.decodeDecompressBody;
        this.useTLS = cc.useTLS;
        this.sockProxyJson = cc.sockProxyJson;
        this.language = cc.language;
        this.namespace = cc.namespace;
        this.autoCleanTopicRouteNotFound = cc.autoCleanTopicRouteNotFound;
        this.recordApiStats = cc.recordApiStats;
        this.disableCallbackExecutor = cc.disableCallbackExecutor;
        this.disableNettyWorkerGroup = cc.disableNettyWorkerGroup;
        this.detectBrokerInterval = cc.detectBrokerInterval;
        this.detectInterval = cc.detectInterval;
        this.detectTimeout = cc.detectTimeout;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.sendLatencyEnable = cc.sendLatencyEnable;
        this.startDetectorEnable = cc.startDetectorEnable;
        this.fetchRemoteClientConfigEnable = cc.fetchRemoteClientConfigEnable;
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.updateConsumeQueueOffsetInterval = updateConsumeQueueOffsetInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.clientConfigInterval = clientConfigInterval;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.unitPara = unitPara;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.decodeReadBody = decodeReadBody;
        cc.decodeDecompressBody = decodeDecompressBody;
        cc.useTLS = useTLS;
        cc.sockProxyJson = sockProxyJson;
        cc.language = language;
        cc.namespace = namespace;
        cc.autoCleanTopicRouteNotFound = autoCleanTopicRouteNotFound;
        cc.recordApiStats = recordApiStats;
        cc.disableNettyWorkerGroup = disableNettyWorkerGroup;
        cc.disableCallbackExecutor = disableCallbackExecutor;
        cc.detectBrokerInterval = detectBrokerInterval;
        cc.detectInterval = detectInterval;
        cc.detectTimeout = detectTimeout;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.sendLatencyEnable = sendLatencyEnable;
        cc.startDetectorEnable = startDetectorEnable;
        cc.fetchRemoteClientConfigEnable = fetchRemoteClientConfigEnable;
        return cc;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public int getPollNameServerInterval() {
        return pollNameServerInterval;
    }

    public int getClientConfigInterval() {
        return clientConfigInterval;
    }

    public int getDetectBrokerInterval() {
        return detectBrokerInterval;
    }

    public void setPollNameServerInterval(int pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }


    public long getPullTimeDelayMillsWhenException() {
        return pullTimeDelayMillsWhenException;
    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }


    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }


    public boolean isAllClientsOffline() {
        return allClientsOffline;
    }

    public void setAllClientsOffline(boolean allClientsOffline) {
        this.allClientsOffline = allClientsOffline;
    }

    public Map<String, String> getUnitPara() {
        return unitPara;
    }

    public void setUnitPara(Map<String, String> unitPara) {
        this.unitPara = unitPara;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(final boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public boolean isDecodeReadBody() {
        return decodeReadBody;
    }

    public void setDecodeReadBody(boolean decodeReadBody) {
        this.decodeReadBody = decodeReadBody;
    }

    public boolean isDecodeDecompressBody() {
        return decodeDecompressBody;
    }

    public void setDecodeDecompressBody(boolean decodeDecompressBody) {
        this.decodeDecompressBody = decodeDecompressBody;
    }

    public String getNamespace() {
        return namespace;
    }

    protected void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public String getSockProxyJson() {
        return sockProxyJson;
    }

    public void setSockProxyJson(String proxyJson) {
        this.sockProxyJson = proxyJson;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public boolean isAutoCleanTopicRouteNotFound() {
        return autoCleanTopicRouteNotFound;
    }

    public void setAutoCleanTopicRouteNotFound(boolean autoCleanTopicRouteNotFound) {
        this.autoCleanTopicRouteNotFound = autoCleanTopicRouteNotFound;
    }

    public boolean isRecordApiStats() {
        return recordApiStats;
    }

    public void setRecordApiStats(boolean recordApiStats) {
        this.recordApiStats = recordApiStats;
    }

    public boolean isDisableCallbackExecutor() {
        return disableCallbackExecutor;
    }

    public void setDisableCallbackExecutor(boolean disableCallbackExecutor) {
        this.disableCallbackExecutor = disableCallbackExecutor;
    }

    public boolean isDisableNettyWorkerGroup() {
        return disableNettyWorkerGroup;
    }

    public void setDisableNettyWorkerGroup(boolean disableNettyWorkerGroup) {
        this.disableNettyWorkerGroup = disableNettyWorkerGroup;
    }

    public boolean isSendLatencyEnable() {
        return sendLatencyEnable;
    }

    public void setSendLatencyEnable(boolean sendLatencyEnable) {
        this.sendLatencyEnable = sendLatencyEnable;
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
    }

    public int getDetectTimeout() {
        return this.detectTimeout;
    }

    public int getDetectInterval() {
        return this.detectInterval;
    }

    @Override
    public String toString() {
        return "ClientConfig{" +
            "namesrvAddr='" + namesrvAddr + '\'' +
            ", clientIP='" + clientIP + '\'' +
            ", instanceName='" + instanceName + '\'' +
            ", namespace='" + namespace + '\'' +
            ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads +
            ", pollNameServerInterval=" + pollNameServerInterval +
            ", updateConsumeQueueOffsetInterval=" + updateConsumeQueueOffsetInterval +
            ", heartbeatBrokerInterval=" + heartbeatBrokerInterval +
            ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval +
            ", clientConfigInterval=" + clientConfigInterval +
            ", detectBrokerInterval=" + detectBrokerInterval +
            ", detectTimeout=" + detectTimeout +
            ", detectInterval=" + detectInterval +
            ", pullTimeDelayMillsWhenException=" + pullTimeDelayMillsWhenException +
            ", unitMode=" + unitMode +
            ", unitName='" + unitName + '\'' +
            ", vipChannelEnabled=" + vipChannelEnabled +
            ", decodeReadBody=" + decodeReadBody +
            ", decodeDecompressBody=" + decodeDecompressBody +
            ", autoCleanTopicRouteNotFound=" + autoCleanTopicRouteNotFound +
            ", recordApiStats=" + recordApiStats +
            ", disableCallbackExecutor=" + disableCallbackExecutor +
            ", disableNettyWorkerGroup=" + disableNettyWorkerGroup +
            ", sendLatencyEnable=" + sendLatencyEnable +
            ", startDetectorEnable=" + startDetectorEnable +
            ", useTLS=" + useTLS +
            ", sockProxyJson=" + sockProxyJson +
            ", language=" + language +
            '}';
    }

    public int getUpdateConsumeQueueOffsetInterval() {
        return updateConsumeQueueOffsetInterval;
    }

    public void setUpdateConsumeQueueOffsetInterval(int updateConsumeQueueOffsetInterval) {
        this.updateConsumeQueueOffsetInterval = updateConsumeQueueOffsetInterval;
    }

    public boolean isFetchRemoteClientConfigEnable() {
        return fetchRemoteClientConfigEnable;
    }

    public void setFetchRemoteClientConfigEnable(boolean fetchRemoteClientConfigEnable) {
        this.fetchRemoteClientConfigEnable = fetchRemoteClientConfigEnable;
    }
}
