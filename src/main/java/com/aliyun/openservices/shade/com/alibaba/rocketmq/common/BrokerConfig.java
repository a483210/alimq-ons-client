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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.annotation.ImportantField;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.constant.LoggerName;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.constant.PermName;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLoggerFactory;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingUtil;

public class BrokerConfig {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    @ImportantField
    private boolean recoverConcurrently = false;

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    @ImportantField
    private String brokerIP1 = RemotingUtil.getLocalAddress();
    private String brokerIP2 = RemotingUtil.getLocalAddress();
    @ImportantField
    private String brokerName = localHostName();
    @ImportantField
    private String brokerClusterName = "DefaultCluster";
    @ImportantField
    private long brokerId = MixAll.MASTER_ID;
    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;
    private int defaultTopicQueueNums = 8;
    @ImportantField
    private boolean autoCreateTopicEnable = true;

    private boolean clusterTopicEnable = true;

    private boolean brokerTopicEnable = true;
    @ImportantField
    private boolean autoCreateSubscriptionGroup = true;
    private String messageStorePlugIn = "";

    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();

    /**
     * thread numbers for send message thread pool, since spin lock will be used by default since 4.0.x, the default
     * value is 1.
     */
    private int sendMessageThreadPoolNums = Math.min(Math.max(64, PROCESSOR_NUMBER), 128);
    private int pullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int litePullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int ackMessageThreadPoolNums = 3;
    private int queryMessageThreadPoolNums = 8 + PROCESSOR_NUMBER;

    private int adminBrokerThreadPoolNums = 32;
    private int clientManageThreadPoolNums = 32;
    private int consumerManageThreadPoolNums = 32;
    private int heartbeatThreadPoolNums = Math.min(32, PROCESSOR_NUMBER);
    private int recoverThreadPoolNums = 32;
    private int maxOffsetThreadPoolNums = 8;

    private int flushConsumerOffsetInterval = 1000 * 5;

    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    @ImportantField
    private boolean rejectTransactionMessage = false;
    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;
    private int sendThreadPoolQueueCapacity = 10000;
    private int pullThreadPoolQueueCapacity = 100000;
    private int ackThreadPoolQueueCapacity = 100000;
    private int queryThreadPoolQueueCapacity = 20000;
    private int clientManagerThreadPoolQueueCapacity = 1000000;
    private int consumerManagerThreadPoolQueueCapacity = 100000;
    private int heartbeatThreadPoolQueueCapacity = 50000;
    private int litePullThreadPoolQueueCapacity = 100000;
    private int maxOffsetThreadPoolQueueCapacity = 10000;
    private int adminBrokerThreadPoolQueueCapacity = 10000;

    private int filterServerNums = 0;

    private boolean longPollingEnable = true;

    private long shortPollingTimeMills = 1000;

    private boolean notifyConsumerIdsChangedEnable = true;

    private boolean highSpeedMode = false;

    // TODO move commercial related variables to ONS
    private boolean commercialEnable = true;
    private int commercialTimerCount = 1;
    private int commercialTransCount = 1;
    private int commercialBigCount = 1;
    private int commercialBaseCount = 1;
    private int commercialSizePerMsg = 4 * 1024;

    private boolean accountStatsEnable = true;
    private boolean accountStatsPrintZeroValues = true;

    private boolean transferMsgByHeap = true;
    private int maxDelayTime = 40;

    private String regionId = MixAll.DEFAULT_TRACE_REGION_ID;
    private int registerBrokerTimeoutMills = 24000;

    private boolean slaveReadEnable = false;

    private boolean disableConsumeIfConsumerReadSlowly = false;
    private long consumerFallbehindThreshold = 1024L * 1024 * 1024 * 16;

    private boolean brokerFastFailureEnable = true;
    private long waitTimeMillsInSendQueue = 200;
    private long waitTimeMillsInPullQueue = 5 * 1000;
    private long waitTimeMillsInLitePullQueue = 5 * 1000;
    private long waitTimeMillsInHeartbeatQueue = 31 * 1000;
    private long waitTimeMillsInAckQueue = 3000;

    private long startAcceptSendRequestTimeStamp = 0L;

    private boolean traceOn = true;

    // Switch of filter bit map calculation.
    // If switch on:
    // 1. Calculate filter bit map when construct queue.
    // 2. Filter bit map will be saved to consume queue extend file if allowed.
    private boolean enableCalcFilterBitMap = false;
    private boolean useLockFreeProducerManager = false;

    //Reject the pull consumer instance to pull messages from broker.
    private boolean rejectPullConsumerEnable = false;

    // Expect num of consumers will use filter.
    private int expectConsumerNumUseFilter = 32;

    // Error rate of bloom filter, 1~100.
    private int maxErrorRateOfBloomFilter = 20;

    //how long to clean filter data after dead.Default: 24h
    private long filterDataCleanTimeSpan = 24 * 3600 * 1000;

    // whether do filter when retry.
    private boolean filterSupportRetry = false;
    private boolean enablePropertyFilter = false;

    private boolean compressedRegister = false;

    private boolean forceRegister = true;

    /**
     * This configurable item defines interval of topics registration of broker to name server. Allowing values are
     * between 10, 000 and 60, 000 milliseconds.
     */
    private int registerNameServerPeriod = 1000 * 30;

    private boolean netWorkFlowController = true;

    private int popPollingSize = 1024;
    private int popPollingMapSize = 100000;
    // 20w cost 200M heap memory.
    private long maxPopPollingSize = 100000;
    private int reviveQueueNum = 8;
    private long reviveInterval = 1000;
    private long reviveMaxSlow = 3;
    private long reviveScanTime = 10000;
    private boolean enablePopLog = false;
    private boolean enablePopBufferMerge = false;
    private int popCkStayBufferTime = 10 * 1000;
    private int popCkStayBufferTimeOut = 3 * 1000;
    private int popCkMaxBufferSize = 200000;
    private int popCkOffsetMaxQueueSize = 20000;

    private boolean realTimeNotifyConsumerChange;

    private boolean generateConfigForScaleOutEnable = false;

    private int pullBatchMaxMessageCount = 160;

    private boolean createHAOrderTopicEnable = false;

    private boolean litePullMessageEnable = false;

    private boolean enableVipIPMapping = false;

    private boolean createRetryTopicImmediately = true;

    private boolean topicValidatorEnable = false;

    private volatile boolean enableBatchSend = false;
    private boolean autoCleanOffsetTable = false;
    private boolean compressTopicAndSubscriptionFile = false;

    private boolean enableConsumePopRetryTopic = false;

    private boolean enableConsumeGlobalNamespacedPopRetryTopic = false;

    private long consumePopRetryTopicInvisibleTime = 3000;

    private boolean useServerSideResetOffset = true;

    private boolean markTaintWhenPopQueueList = true;

    private long updateTaintQueueOffsetSamplingTimes = 1000;

    private long taintClearTimeSeconds = 15;

    // Send message to system topic directly may trigger undefined behavior, for example:
    // https://yuque.antfin-inc.com/aone709911/vqtokp/xqfqmg
    // Enable this config to reject these requests
    private boolean rejectSendMessageToSystemTopicDirectly = false;

    // To avoid a potential security issue: verify the retrieved message in send message back request
    private boolean rejectUnauthorizedSendMessageBackRequest = false;

    private boolean enableBroadcastOffsetStore = true;

    private long broadcastOffsetExpireSecond = 3 * 60;

    private long broadcastOffsetExpireMaxSecond = 15 * 60;

    public boolean isCompressTopicAndSubscriptionFile() {
        return compressTopicAndSubscriptionFile;
    }

    public void setCompressTopicAndSubscriptionFile(boolean compressTopicAndSubscriptionFile) {
        this.compressTopicAndSubscriptionFile = compressTopicAndSubscriptionFile;
    }

    public boolean isAutoCleanOffsetTable() {
        return autoCleanOffsetTable;
    }

    public void setAutoCleanOffsetTable(boolean autoCleanOffsetTable) {
        this.autoCleanOffsetTable = autoCleanOffsetTable;
    }

    /**
     * Estimate accumulation or not when subscription filter type is tag and is not SUB_ALL.
     */
    private boolean estimateAccumulation = false;

    public long getMaxPopPollingSize() {
        return maxPopPollingSize;
    }

    public void setMaxPopPollingSize(long maxPopPollingSize) {
        this.maxPopPollingSize = maxPopPollingSize;
    }

    public int getPopPollingMapSize() {
        return popPollingMapSize;
    }

    public void setPopPollingMapSize(int popPollingMapSize) {
        this.popPollingMapSize = popPollingMapSize;
    }

    public long getReviveScanTime() {
        return reviveScanTime;
    }

    public void setReviveScanTime(long reviveScanTime) {
        this.reviveScanTime = reviveScanTime;
    }

    public long getReviveMaxSlow() {
        return reviveMaxSlow;
    }

    public void setReviveMaxSlow(long reviveMaxSlow) {
        this.reviveMaxSlow = reviveMaxSlow;
    }

    public boolean isEnablePopLog() {
        return enablePopLog;
    }

    public void setEnablePopLog(boolean enablePopLog) {
        this.enablePopLog = enablePopLog;
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(final boolean traceOn) {
        this.traceOn = traceOn;
    }

    public long getStartAcceptSendRequestTimeStamp() {
        return startAcceptSendRequestTimeStamp;
    }

    public void setStartAcceptSendRequestTimeStamp(final long startAcceptSendRequestTimeStamp) {
        this.startAcceptSendRequestTimeStamp = startAcceptSendRequestTimeStamp;
    }

    public long getWaitTimeMillsInSendQueue() {
        return waitTimeMillsInSendQueue;
    }

    public void setWaitTimeMillsInSendQueue(final long waitTimeMillsInSendQueue) {
        this.waitTimeMillsInSendQueue = waitTimeMillsInSendQueue;
    }

    public long getWaitTimeMillsInAckQueue() {
        return waitTimeMillsInAckQueue;
    }

    public void setWaitTimeMillsInAckQueue(long waitTimeMillsInAckQueue) {
        this.waitTimeMillsInAckQueue = waitTimeMillsInAckQueue;
    }

    public long getConsumerFallbehindThreshold() {
        return consumerFallbehindThreshold;
    }

    public void setConsumerFallbehindThreshold(final long consumerFallbehindThreshold) {
        this.consumerFallbehindThreshold = consumerFallbehindThreshold;
    }

    public boolean isBrokerFastFailureEnable() {
        return brokerFastFailureEnable;
    }

    public void setBrokerFastFailureEnable(final boolean brokerFastFailureEnable) {
        this.brokerFastFailureEnable = brokerFastFailureEnable;
    }

    public long getWaitTimeMillsInPullQueue() {
        return waitTimeMillsInPullQueue;
    }

    public void setWaitTimeMillsInPullQueue(final long waitTimeMillsInPullQueue) {
        this.waitTimeMillsInPullQueue = waitTimeMillsInPullQueue;
    }

    public boolean isDisableConsumeIfConsumerReadSlowly() {
        return disableConsumeIfConsumerReadSlowly;
    }

    public void setDisableConsumeIfConsumerReadSlowly(final boolean disableConsumeIfConsumerReadSlowly) {
        this.disableConsumeIfConsumerReadSlowly = disableConsumeIfConsumerReadSlowly;
    }

    public boolean isSlaveReadEnable() {
        return slaveReadEnable;
    }

    public void setSlaveReadEnable(final boolean slaveReadEnable) {
        this.slaveReadEnable = slaveReadEnable;
    }

    public static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Failed to obtain the host name", e);
        }

        return "DEFAULT_BROKER";
    }

    public int getRegisterBrokerTimeoutMills() {
        return registerBrokerTimeoutMills;
    }

    public void setRegisterBrokerTimeoutMills(final int registerBrokerTimeoutMills) {
        this.registerBrokerTimeoutMills = registerBrokerTimeoutMills;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(final String regionId) {
        this.regionId = regionId;
    }

    public boolean isTransferMsgByHeap() {
        return transferMsgByHeap;
    }

    public void setTransferMsgByHeap(final boolean transferMsgByHeap) {
        this.transferMsgByHeap = transferMsgByHeap;
    }

    public String getMessageStorePlugIn() {
        return messageStorePlugIn;
    }

    public void setMessageStorePlugIn(String messageStorePlugIn) {
        this.messageStorePlugIn = messageStorePlugIn;
    }

    public boolean isHighSpeedMode() {
        return highSpeedMode;
    }

    public void setHighSpeedMode(final boolean highSpeedMode) {
        this.highSpeedMode = highSpeedMode;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getBrokerPermission() {
        return brokerPermission;
    }

    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }

    public void setAutoCreateTopicEnable(boolean autoCreateTopic) {
        this.autoCreateTopicEnable = autoCreateTopic;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public String getBrokerIP2() {
        return brokerIP2;
    }

    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }

    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }

    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }

    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }

    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }

    public int getAckMessageThreadPoolNums() {
        return ackMessageThreadPoolNums;
    }

    public void setAckMessageThreadPoolNums(int ackMessageThreadPoolNums) {
        this.ackMessageThreadPoolNums = ackMessageThreadPoolNums;
    }

    public int getQueryMessageThreadPoolNums() {
        return queryMessageThreadPoolNums;
    }

    public void setQueryMessageThreadPoolNums(final int queryMessageThreadPoolNums) {
        this.queryMessageThreadPoolNums = queryMessageThreadPoolNums;
    }

    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }

    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }

    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }

    public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
        this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
    }

    public int getFlushConsumerOffsetHistoryInterval() {
        return flushConsumerOffsetHistoryInterval;
    }

    public void setFlushConsumerOffsetHistoryInterval(int flushConsumerOffsetHistoryInterval) {
        this.flushConsumerOffsetHistoryInterval = flushConsumerOffsetHistoryInterval;
    }

    public boolean isClusterTopicEnable() {
        return clusterTopicEnable;
    }

    public void setClusterTopicEnable(boolean clusterTopicEnable) {
        this.clusterTopicEnable = clusterTopicEnable;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }

    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }

    public boolean isRejectTransactionMessage() {
        return rejectTransactionMessage;
    }

    public void setRejectTransactionMessage(boolean rejectTransactionMessage) {
        this.rejectTransactionMessage = rejectTransactionMessage;
    }

    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }

    public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
        this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
    }

    public int getSendThreadPoolQueueCapacity() {
        return sendThreadPoolQueueCapacity;
    }

    public void setSendThreadPoolQueueCapacity(int sendThreadPoolQueueCapacity) {
        this.sendThreadPoolQueueCapacity = sendThreadPoolQueueCapacity;
    }

    public int getPullThreadPoolQueueCapacity() {
        return pullThreadPoolQueueCapacity;
    }

    public void setPullThreadPoolQueueCapacity(int pullThreadPoolQueueCapacity) {
        this.pullThreadPoolQueueCapacity = pullThreadPoolQueueCapacity;
    }

    public int getAckThreadPoolQueueCapacity() {
        return ackThreadPoolQueueCapacity;
    }

    public void setAckThreadPoolQueueCapacity(int ackThreadPoolQueueCapacity) {
        this.ackThreadPoolQueueCapacity = ackThreadPoolQueueCapacity;
    }

    public int getQueryThreadPoolQueueCapacity() {
        return queryThreadPoolQueueCapacity;
    }

    public void setQueryThreadPoolQueueCapacity(final int queryThreadPoolQueueCapacity) {
        this.queryThreadPoolQueueCapacity = queryThreadPoolQueueCapacity;
    }

    public boolean isBrokerTopicEnable() {
        return brokerTopicEnable;
    }

    public void setBrokerTopicEnable(boolean brokerTopicEnable) {
        this.brokerTopicEnable = brokerTopicEnable;
    }

    public int getFilterServerNums() {
        return filterServerNums;
    }

    public void setFilterServerNums(int filterServerNums) {
        this.filterServerNums = filterServerNums;
    }

    public boolean isLongPollingEnable() {
        return longPollingEnable;
    }

    public void setLongPollingEnable(boolean longPollingEnable) {
        this.longPollingEnable = longPollingEnable;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    public long getShortPollingTimeMills() {
        return shortPollingTimeMills;
    }

    public void setShortPollingTimeMills(long shortPollingTimeMills) {
        this.shortPollingTimeMills = shortPollingTimeMills;
    }

    public int getClientManageThreadPoolNums() {
        return clientManageThreadPoolNums;
    }

    public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
        this.clientManageThreadPoolNums = clientManageThreadPoolNums;
    }

    public boolean isCommercialEnable() {
        return commercialEnable;
    }

    public void setCommercialEnable(final boolean commercialEnable) {
        this.commercialEnable = commercialEnable;
    }

    public int getCommercialTimerCount() {
        return commercialTimerCount;
    }

    public void setCommercialTimerCount(final int commercialTimerCount) {
        this.commercialTimerCount = commercialTimerCount;
    }

    public int getCommercialTransCount() {
        return commercialTransCount;
    }

    public void setCommercialTransCount(final int commercialTransCount) {
        this.commercialTransCount = commercialTransCount;
    }

    public int getCommercialBigCount() {
        return commercialBigCount;
    }

    public void setCommercialBigCount(final int commercialBigCount) {
        this.commercialBigCount = commercialBigCount;
    }

    public int getMaxDelayTime() {
        return maxDelayTime;
    }

    public void setMaxDelayTime(final int maxDelayTime) {
        this.maxDelayTime = maxDelayTime;
    }

    public int getClientManagerThreadPoolQueueCapacity() {
        return clientManagerThreadPoolQueueCapacity;
    }

    public void setClientManagerThreadPoolQueueCapacity(int clientManagerThreadPoolQueueCapacity) {
        this.clientManagerThreadPoolQueueCapacity = clientManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManagerThreadPoolQueueCapacity() {
        return consumerManagerThreadPoolQueueCapacity;
    }

    public void setConsumerManagerThreadPoolQueueCapacity(int consumerManagerThreadPoolQueueCapacity) {
        this.consumerManagerThreadPoolQueueCapacity = consumerManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManageThreadPoolNums() {
        return consumerManageThreadPoolNums;
    }

    public void setConsumerManageThreadPoolNums(int consumerManageThreadPoolNums) {
        this.consumerManageThreadPoolNums = consumerManageThreadPoolNums;
    }

    public int getCommercialBaseCount() {
        return commercialBaseCount;
    }

    public void setCommercialBaseCount(int commercialBaseCount) {
        this.commercialBaseCount = commercialBaseCount;
    }

    public int getCommercialSizePerMsg() {
        return commercialSizePerMsg;
    }

    public void setCommercialSizePerMsg(int commercialSizePerMsg) {
        this.commercialSizePerMsg = commercialSizePerMsg;
    }

    public boolean isAccountStatsEnable() {
        return accountStatsEnable;
    }

    public void setAccountStatsEnable(boolean accountStatsEnable) {
        this.accountStatsEnable = accountStatsEnable;
    }

    public boolean isAccountStatsPrintZeroValues() {
        return accountStatsPrintZeroValues;
    }

    public void setAccountStatsPrintZeroValues(boolean accountStatsPrintZeroValues) {
        this.accountStatsPrintZeroValues = accountStatsPrintZeroValues;
    }

    public boolean isEnableCalcFilterBitMap() {
        return enableCalcFilterBitMap;
    }

    public void setEnableCalcFilterBitMap(boolean enableCalcFilterBitMap) {
        this.enableCalcFilterBitMap = enableCalcFilterBitMap;
    }

    public int getExpectConsumerNumUseFilter() {
        return expectConsumerNumUseFilter;
    }

    public void setExpectConsumerNumUseFilter(int expectConsumerNumUseFilter) {
        this.expectConsumerNumUseFilter = expectConsumerNumUseFilter;
    }

    public int getMaxErrorRateOfBloomFilter() {
        return maxErrorRateOfBloomFilter;
    }

    public void setMaxErrorRateOfBloomFilter(int maxErrorRateOfBloomFilter) {
        this.maxErrorRateOfBloomFilter = maxErrorRateOfBloomFilter;
    }

    public long getFilterDataCleanTimeSpan() {
        return filterDataCleanTimeSpan;
    }

    public void setFilterDataCleanTimeSpan(long filterDataCleanTimeSpan) {
        this.filterDataCleanTimeSpan = filterDataCleanTimeSpan;
    }

    public boolean isFilterSupportRetry() {
        return filterSupportRetry;
    }

    public void setFilterSupportRetry(boolean filterSupportRetry) {
        this.filterSupportRetry = filterSupportRetry;
    }

    public boolean isEnablePropertyFilter() {
        return enablePropertyFilter;
    }

    public void setEnablePropertyFilter(boolean enablePropertyFilter) {
        this.enablePropertyFilter = enablePropertyFilter;
    }

    public void setPopPollingSize(int popPollingSize) {
        this.popPollingSize = popPollingSize;
    }

    public int getPopPollingSize() {
        return popPollingSize;
    }

    public boolean isUseLockFreeProducerManager() {
        return useLockFreeProducerManager;
    }

    public void setUseLockFreeProducerManager(boolean useLockFreeProducerManager) {
        this.useLockFreeProducerManager = useLockFreeProducerManager;
    }

    public boolean isRejectPullConsumerEnable() {
        return rejectPullConsumerEnable;
    }

    public void setRejectPullConsumerEnable(final boolean rejectPullConsumerEnable) {
        this.rejectPullConsumerEnable = rejectPullConsumerEnable;
    }

    public int getReviveQueueNum() {
        return reviveQueueNum;
    }

    public void setReviveQueueNum(int reviveQueueNum) {
        this.reviveQueueNum = reviveQueueNum;
    }

    public long getReviveInterval() {
        return reviveInterval;
    }

    public void setReviveInterval(long reviveInterval) {
        this.reviveInterval = reviveInterval;
    }

    public int getPopCkStayBufferTime() {
        return popCkStayBufferTime;
    }

    public void setPopCkStayBufferTime(int popCkStayBufferTime) {
        this.popCkStayBufferTime = popCkStayBufferTime;
    }

    public int getPopCkStayBufferTimeOut() {
        return popCkStayBufferTimeOut;
    }

    public void setPopCkStayBufferTimeOut(int popCkStayBufferTimeOut) {
        this.popCkStayBufferTimeOut = popCkStayBufferTimeOut;
    }

    public boolean isEnablePopBufferMerge() {
        return enablePopBufferMerge;
    }

    public void setEnablePopBufferMerge(boolean enablePopBufferMerge) {
        this.enablePopBufferMerge = enablePopBufferMerge;
    }

    public int getPopCkMaxBufferSize() {
        return popCkMaxBufferSize;
    }

    public void setPopCkMaxBufferSize(int popCkMaxBufferSize) {
        this.popCkMaxBufferSize = popCkMaxBufferSize;
    }

    public int getPopCkOffsetMaxQueueSize() {
        return popCkOffsetMaxQueueSize;
    }

    public void setPopCkOffsetMaxQueueSize(int popCkOffsetMaxQueueSize) {
        this.popCkOffsetMaxQueueSize = popCkOffsetMaxQueueSize;
    }

    public boolean isCompressedRegister() {
        return compressedRegister;
    }

    public void setCompressedRegister(boolean compressedRegister) {
        this.compressedRegister = compressedRegister;
    }

    public boolean isForceRegister() {
        return forceRegister;
    }

    public void setForceRegister(boolean forceRegister) {
        this.forceRegister = forceRegister;
    }

    public int getHeartbeatThreadPoolQueueCapacity() {
        return heartbeatThreadPoolQueueCapacity;
    }

    public void setHeartbeatThreadPoolQueueCapacity(int heartbeatThreadPoolQueueCapacity) {
        this.heartbeatThreadPoolQueueCapacity = heartbeatThreadPoolQueueCapacity;
    }

    public int getHeartbeatThreadPoolNums() {
        return heartbeatThreadPoolNums;
    }

    public void setHeartbeatThreadPoolNums(int heartbeatThreadPoolNums) {
        this.heartbeatThreadPoolNums = heartbeatThreadPoolNums;
    }


    public int getRecoverThreadPoolNums() {
        return recoverThreadPoolNums;
    }

    public void setRecoverThreadPoolNums(int recoverThreadPoolNums) {
        this.recoverThreadPoolNums = recoverThreadPoolNums;
    }

    public long getWaitTimeMillsInHeartbeatQueue() {
        return waitTimeMillsInHeartbeatQueue;
    }

    public void setWaitTimeMillsInHeartbeatQueue(long waitTimeMillsInHeartbeatQueue) {
        this.waitTimeMillsInHeartbeatQueue = waitTimeMillsInHeartbeatQueue;
    }

    public int getRegisterNameServerPeriod() {
        return registerNameServerPeriod;
    }

    public void setRegisterNameServerPeriod(int registerNameServerPeriod) {
        this.registerNameServerPeriod = registerNameServerPeriod;
    }

    public boolean isNetWorkFlowController() {
        return netWorkFlowController;
    }

    public void setNetWorkFlowController(boolean netWorkFlowController) {
        this.netWorkFlowController = netWorkFlowController;
    }

    public int getPullBatchMaxMessageCount() {
        return pullBatchMaxMessageCount;
    }

    public void setPullBatchMaxMessageCount(int pullBatchMaxMessageCount) {
        this.pullBatchMaxMessageCount = pullBatchMaxMessageCount;
    }

    public boolean isRealTimeNotifyConsumerChange() {
        return realTimeNotifyConsumerChange;
    }

    public void setRealTimeNotifyConsumerChange(boolean realTimeNotifyConsumerChange) {
        this.realTimeNotifyConsumerChange = realTimeNotifyConsumerChange;
    }

    public boolean isCreateHAOrderTopicEnable() {
        return createHAOrderTopicEnable;
    }

    public void setCreateHAOrderTopicEnable(boolean createHAOrderTopicEnable) {
        this.createHAOrderTopicEnable = createHAOrderTopicEnable;
    }

    public boolean isLitePullMessageEnable() {
        return litePullMessageEnable;
    }

    public void setLitePullMessageEnable(boolean litePullMessageEnable) {
        this.litePullMessageEnable = litePullMessageEnable;
    }

    public int getLitePullThreadPoolQueueCapacity() {
        return litePullThreadPoolQueueCapacity;
    }

    public void setLitePullThreadPoolQueueCapacity(int litePullThreadPoolQueueCapacity) {
        this.litePullThreadPoolQueueCapacity = litePullThreadPoolQueueCapacity;
    }

    public int getLitePullMessageThreadPoolNums() {
        return litePullMessageThreadPoolNums;
    }

    public void setLitePullMessageThreadPoolNums(int litePullMessageThreadPoolNums) {
        this.litePullMessageThreadPoolNums = litePullMessageThreadPoolNums;
    }

    public long getWaitTimeMillsInLitePullQueue() {
        return waitTimeMillsInLitePullQueue;
    }

    public void setWaitTimeMillsInLitePullQueue(long waitTimeMillsInLitePullQueue) {
        this.waitTimeMillsInLitePullQueue = waitTimeMillsInLitePullQueue;
    }

    public boolean isGenerateConfigForScaleOutEnable() { return generateConfigForScaleOutEnable; }

    public void setGenerateConfigForScaleOutEnable(boolean generateConfigForScaleOutEnable) {
        this.generateConfigForScaleOutEnable = generateConfigForScaleOutEnable;
    }

    public boolean isRecoverConcurrently() {
        return recoverConcurrently;
    }

    public void setRecoverConcurrently(boolean recoverConcurrently) {
        this.recoverConcurrently = recoverConcurrently;
    }

    public boolean isTopicValidatorEnable() {
        return topicValidatorEnable;
    }

    public void setTopicValidatorEnable(boolean topicValidatorEnable) {
        this.topicValidatorEnable = topicValidatorEnable;
    }

    public int getMaxOffsetThreadPoolQueueCapacity() {
        return maxOffsetThreadPoolQueueCapacity;
    }

    public void setMaxOffsetThreadPoolQueueCapacity(int maxOffsetThreadPoolQueueCapacity) {
        this.maxOffsetThreadPoolQueueCapacity = maxOffsetThreadPoolQueueCapacity;
    }

    public int getAdminBrokerThreadPoolQueueCapacity() {
        return adminBrokerThreadPoolQueueCapacity;
    }

    public void setAdminBrokerThreadPoolQueueCapacity(int adminBrokerThreadPoolQueueCapacity) {
        this.adminBrokerThreadPoolQueueCapacity = adminBrokerThreadPoolQueueCapacity;
    }

    public int getMaxOffsetThreadPoolNums() {
        return maxOffsetThreadPoolNums;
    }

    public void setMaxOffsetThreadPoolNums(int maxOffsetThreadPoolNums) {
        this.maxOffsetThreadPoolNums = maxOffsetThreadPoolNums;
    }

    public boolean isEnableBatchSend() {
        return enableBatchSend;
    }

    public void setEnableBatchSend(boolean enableBatchSend) {
        this.enableBatchSend = enableBatchSend;
    }

    public boolean isEstimateAccumulation() {
        return estimateAccumulation;
    }

    public void setEstimateAccumulation(boolean estimateAccumulation) {
        this.estimateAccumulation = estimateAccumulation;
    }

    public boolean isEnableConsumePopRetryTopic() {
        return enableConsumePopRetryTopic;
    }

    public void setEnableConsumePopRetryTopic(boolean enableConsumePopRetryTopic) {
        this.enableConsumePopRetryTopic = enableConsumePopRetryTopic;
    }

    public long getConsumePopRetryTopicInvisibleTime() {
        return consumePopRetryTopicInvisibleTime;
    }

    public void setConsumePopRetryTopicInvisibleTime(long consumePopRetryTopicInvisibleTime) {
        this.consumePopRetryTopicInvisibleTime = consumePopRetryTopicInvisibleTime;
    }

    public boolean isUseServerSideResetOffset() {
        return useServerSideResetOffset;
    }

    public void setUseServerSideResetOffset(boolean useServerSideResetOffset) {
        this.useServerSideResetOffset = useServerSideResetOffset;
    }

    public boolean isEnableConsumeGlobalNamespacedPopRetryTopic() {
        return enableConsumeGlobalNamespacedPopRetryTopic;
    }

    public void setEnableConsumeGlobalNamespacedPopRetryTopic(boolean enableConsumeGlobalNamespacedPopRetryTopic) {
        this.enableConsumeGlobalNamespacedPopRetryTopic = enableConsumeGlobalNamespacedPopRetryTopic;
    }

    public boolean isCreateRetryTopicImmediately() {
        return createRetryTopicImmediately;
    }

    public void setCreateRetryTopicImmediately(boolean createRetryTopicImmediately) {
        this.createRetryTopicImmediately = createRetryTopicImmediately;
    }

    public boolean isMarkTaintWhenPopQueueList() {
        return markTaintWhenPopQueueList;
    }

    public void setMarkTaintWhenPopQueueList(boolean markTaintWhenPopQueueList) {
        this.markTaintWhenPopQueueList = markTaintWhenPopQueueList;
    }

    public long getUpdateTaintQueueOffsetSamplingTimes() {
        return updateTaintQueueOffsetSamplingTimes;
    }

    public void setUpdateTaintQueueOffsetSamplingTimes(long updateTaintQueueOffsetSamplingTimes) {
        this.updateTaintQueueOffsetSamplingTimes = updateTaintQueueOffsetSamplingTimes;
    }

    public boolean isRejectSendMessageToSystemTopicDirectly() {
        return rejectSendMessageToSystemTopicDirectly;
    }

    public void setRejectSendMessageToSystemTopicDirectly(boolean rejectSendMessageToSystemTopicDirectly) {
        this.rejectSendMessageToSystemTopicDirectly = rejectSendMessageToSystemTopicDirectly;
    }

    public boolean isRejectUnauthorizedSendMessageBackRequest() {
        return rejectUnauthorizedSendMessageBackRequest;
    }

    public void setRejectUnauthorizedSendMessageBackRequest(boolean rejectUnauthorizedSendMessageBackRequest) {
        this.rejectUnauthorizedSendMessageBackRequest = rejectUnauthorizedSendMessageBackRequest;
    }

    public long getTaintClearTimeSeconds() {
        return taintClearTimeSeconds;
    }

    public void setTaintClearTimeSeconds(long taintClearTimeSeconds) {
        this.taintClearTimeSeconds = taintClearTimeSeconds;
    }

    public boolean isEnableVipIPMapping() {
        return enableVipIPMapping;
    }

    public void setEnableVipIPMapping(boolean enableVipIPMapping) {
        this.enableVipIPMapping = enableVipIPMapping;
    }

    public boolean isEnableBroadcastOffsetStore() {
        return enableBroadcastOffsetStore;
    }

    public void setEnableBroadcastOffsetStore(boolean enableBroadcastOffsetStore) {
        this.enableBroadcastOffsetStore = enableBroadcastOffsetStore;
    }

    public long getBroadcastOffsetExpireSecond() {
        return broadcastOffsetExpireSecond;
    }

    public void setBroadcastOffsetExpireSecond(long broadcastOffsetExpireSecond) {
        this.broadcastOffsetExpireSecond = broadcastOffsetExpireSecond;
    }

    public long getBroadcastOffsetExpireMaxSecond() {
        return broadcastOffsetExpireMaxSecond;
    }

    public void setBroadcastOffsetExpireMaxSecond(long broadcastOffsetExpireMaxSecond) {
        this.broadcastOffsetExpireMaxSecond = broadcastOffsetExpireMaxSecond;
    }
}
