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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.factory;

import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.ClientConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.RemoteClientConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.admin.MQAdminExtInner;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.ClientRemotingProcessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.MQAdminImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.MQClientAPIImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.MQClientManager;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.MQConsumerInner;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.ProcessQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.PullMessageService;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.RebalanceService;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.MQProducerInner;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.ordermessage.OrderMessageHandler;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.stat.ConsumerStatsManager;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MQVersion;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ServiceState;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.constant.PermName;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.ConsumerData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.ProducerData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.QueueDesc;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RPCHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.lang3.StringUtils;

public class MQClientInstance {
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final static InternalLogger LOG = ClientLogger.getLog();
    private final ClientConfig clientConfig;
    private final RemoteClientConfig remoteClientConfig = new RemoteClientConfig();
    private final int instanceIndex;
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();
    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();
    private final NettyClientConfig nettyClientConfig;
    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    /**
     * <Topic, <Key, Offset>>
     */
    private final ConcurrentMap<String, ConcurrentMap<String, Long>> topicQueueOffsetTable = new ConcurrentHashMap<String, ConcurrentMap<String, Long>>();
    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<String, HashMap<Long, String>>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
        new ConcurrentHashMap<String, HashMap<String, Integer>>();
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryImpl("MQClientFactoryScheduledThread", false));
    private final ScheduledExecutorService fetchRemoteConfigExecutorService = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryImpl("MQClientFactoryFetchRemoteConfigScheduledThread", false));
    private final ClientRemotingProcessor clientRemotingProcessor;
    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;
    private final DefaultMQProducer defaultMQProducer;
    private final ConsumerStatsManager consumerStatsManager;
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private DatagramSocket datagramSocket;
    private Random random = new Random();

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this(clientConfig, instanceIndex, clientId, rpcHook, null, null);
    }

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook,
                            final EventLoopGroup eventLoopGroup, final EventExecutorGroup eventExecutorGroup) {
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
        this.nettyClientConfig.setSockProxyJson(clientConfig.getSockProxyJson());
        this.nettyClientConfig.setDisableCallbackExecutor(clientConfig.isDisableCallbackExecutor());
        this.nettyClientConfig.setDisableNettyWorkerGroup(clientConfig.isDisableNettyWorkerGroup());
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig, eventLoopGroup, eventExecutorGroup);

        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            LOG.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);
        this.mQClientAPIImpl.setConsumerStatsManager(consumerStatsManager);
        LOG.info("created a new client Instance, FactoryIndex: {} ClinetID: {} {} {}, serializeType={}",
            this.instanceIndex,
            this.clientId,
            this.clientConfig,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        String orderConf = route.getOrderTopicConf();
        if (!route.isHAOrderTopic() && orderConf != null && orderConf.length() > 0) {
            String[] brokers = orderConf.split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        } else {
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        continue;
                    }

                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }

                    if (route.isHAOrderTopic()) {
                        if (qd.getQueueDescList() != null) {
                            List<MessageQueue> messageQueueList = info.getMessageQueueList();
                            for (QueueDesc queueDesc : qd.getQueueDescList()) {
                                if (queueDesc.getQueueGroupId() < route.getWriteQueueGroupNums()) {
                                    messageQueueList.add(
                                        new MessageQueue(topic, qd.getBrokerName(), queueDesc.getMessageQueueId(), queueDesc.getQueueGroupId(), queueDesc.isMainQueue())
                                    );
                                    if (queueDesc.isMainQueue()) {
                                        info.setMainQueuePreferred(true);
                                    }
                                }
                            }
                        }
                    } else {
                        for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                            MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                            info.getMessageQueueList().add(mq);
                        }
                    }

                }
            }

            if (route.isHAOrderTopic()) {
                info.setOrderTopic(true);
            } else {
                info.setOrderTopic(false);
            }
        }

        return info;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();
        List<QueueData> qds = route.getQueueDatas();
        List<QueueDesc> qs = null;
        Map<Integer, List<MessageQueue>> mqMap = new HashMap<Integer, List<MessageQueue>>();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                if (route.isHAOrderTopic()) {
                    if ((qs = qd.getQueueDescList()) != null) {
                        for (QueueDesc queueDesc : qs) {
                            if (queueDesc.getQueueGroupId() < route.getReadQueueGroupNums()) {
                                MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), queueDesc.getMessageQueueId(), queueDesc.getQueueGroupId(), queueDesc.isMainQueue());
                                if (!mqMap.containsKey(mq.getQueueGroupId())) {
                                    mqMap.put(mq.getQueueGroupId(), new ArrayList<MessageQueue>());
                                }
                                mqMap.get(mq.getQueueGroupId()).add(mq);
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < qd.getReadQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        mqList.add(mq);
                    }
                }
            }
        }
        Iterator<Map.Entry<Integer, List<MessageQueue>>> it = mqMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, List<MessageQueue>> entry = it.next();
            if (entry.getValue().size() != route.getOrderTopicQueueGroupSize()) {
                LOG.warn("Topic: {}, group id: {}. QueueGroup incomplete, expect size: {}, actual size: {}",
                    topic, entry.getKey(), route.getOrderTopicQueueGroupSize(), entry.getValue().size());
                it.remove();
            } else {
                mqList.addAll(entry.getValue());
            }
        }

        return mqList;
    }

    public void start() throws MQClientException {

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    this.mQClientAPIImpl.start();
                    // Start various schedule tasks
                    this.startScheduledTask();
                    // Start pull service
                    this.pullMessageService.start();
                    // Start rebalance service
                    this.rebalanceService.start();
                    // Start push service
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    LOG.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    private void startScheduledTask() {
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        LOG.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    LOG.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

        if (this.clientConfig.isFetchRemoteClientConfigEnable()) {
            this.fetchRemoteConfigExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        boolean lastOffline = remoteClientConfig.isOffline();
                        Properties property = MQClientInstance.this.mQClientAPIImpl.getRemoteClientConfig(LOCK_TIMEOUT_MILLIS);
                        MixAll.properties2Object(property, remoteClientConfig);
                        LOG.warn("update offline config, {}, {} -> {}", MQClientInstance.this.clientId, lastOffline, remoteClientConfig.isOffline());
                        if (remoteClientConfig.isOffline()) {
                            allClientsOffline();
                        } else {
                            allClientsOnline();
                        }
                    } catch (Exception e) {
                        LOG.warn("ScheduledTask getRemoteClientConfig failed, " + e.getMessage());
                    }
                }
            }, 10, this.clientConfig.getClientConfigInterval(), TimeUnit.MILLISECONDS);
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override public void run() {
                if (MQClientInstance.this.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState() == ServiceState.RUNNING) {
                    try {
                        MQClientInstance.this.fetchConsumeQueueOffsetFromBroker();
                    } catch (Exception e) {
                        LOG.warn("ScheduledTask updateConsumeQueueOffsetFromBroker failed, " + e.getMessage());
                    }
                }
            }
        }, 10, this.clientConfig.getUpdateConsumeQueueOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    LOG.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    LOG.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    LOG.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String getClientId() {
        return clientId;
    }

    public RemoteClientConfig getRemoteClientConfig() {
        return this.remoteClientConfig;
    }

    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<String>();

        // Consumer
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                String consumerGroup = entry.getKey();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                    if (impl instanceof DefaultMQPullConsumerImpl) {
                        DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
                        if (consumer.getDefaultMQPullConsumer().isAutoUpdateTopicRoute()) {
                            topicList.addAll(consumer.getRebalanceImpl().getSubscriptionInner().keySet());
                        }
                    }
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        String retryTopic = MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
                        topicList.add(retryTopic);
                    }
                }
            }
        }

        // Producer
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    public void fetchConsumeQueueOffsetFromBroker() throws MQClientException {
        for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                Set<String> list = impl.getPublishTopicList();
                for (String topic : list) {
                    TopicPublishInfo info = impl.getTopicPublishInfo(topic);
                    if (info.isHAOrderTopic()) {
                        this.fetchConsumeQueueOffsetFromBroker(info);
                    }
                }
            }
        }
    }

    /**
     * Fetch all the queues' offset for a topic from brokers.
     *
     * @param info The object topicPublishInfo that needs to be updated.
     */
    public void fetchConsumeQueueOffsetFromBroker(TopicPublishInfo info) throws MQClientException {
        if (info == null) {
            return;
        }
        for (MessageQueue mq : info.getMessageQueueList()) {
            String topic = mq.getTopic();
            long newOffset = this.getDefaultMQProducer().getDefaultMQProducerImpl().maxOffset(mq, false);
            this.putQueueOffset(topic, mq.generateKey(), newOffset);
        }
    }

    /**
     * @param offsetTable
     * @param namespace
     * @return newOffsetTable
     */
    public Map<MessageQueue, Long> parseOffsetTableFromBroker(Map<MessageQueue, Long> offsetTable, String namespace) {
        HashMap<MessageQueue, Long> newOffsetTable = new HashMap<MessageQueue, Long>();
        if (StringUtils.isNotEmpty(namespace)) {
            for (Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
                MessageQueue queue = entry.getKey();
                queue.setTopic(NamespaceUtil.withoutNamespace(queue.getTopic(), namespace));
                newOffsetTable.put(queue, entry.getValue());
            }
        } else {
            newOffsetTable.putAll(offsetTable);
        }

        return newOffsetTable;
    }

    /**
     * Remove offline broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                LOG.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            LOG.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("cleanOfflineBroker Exception", e);
        }
    }

    public void checkClientInBroker() throws MQClientException {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    try {
                        this.getMQClientAPIImpl().checkClientInBroker(
                            addr, entry.getKey(), this.clientId, subscriptionData, 3 * 1000
                        );
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            throw new MQClientException("Check client in broker error, maybe because you use "
                                + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!"
                                + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                "have use the new features which are not supported by server, please check the log!", e);
                        }
                    }
                }
            }
        }
    }

    public void sendHeartbeatToAllBrokerWithTimedLock() {
        try {
            if (this.lockHeartbeat.tryLock(3000, TimeUnit.MILLISECONDS)) {
                try {
                    this.sendHeartbeatToAllBroker();
                    this.uploadFilterClassSource();
                } catch (final Exception e) {
                    LOG.error("sendHeartbeatToAllBroker exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                LOG.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            LOG.warn("sendHeartbeatToAllBrokerWithTimedLock exception", e);
        }
    }

    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                this.sendHeartbeatToAllBroker();
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                LOG.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            LOG.warn("lock heartBeat, but failed.");
        }
    }

    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (!impl.isOffline()) {
                impl.persistConsumerOffset();
            }
        }
    }

    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> entry = it.next();
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private void sendHeartbeatToAllBroker() {
        // If Broker is not readable and writable, do not send heartbeat.
        Set<String> activeBrokerNameSet = new HashSet<String>();
        Set<String> activeTopicSet = new HashSet<String>();
        for (String topic : topicRouteTable.keySet()) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            if (null == topicRouteData) {
                continue;
            }
            List<QueueData> queueDataList = topicRouteData.getQueueDatas();
            if (null == queueDataList) {
                continue;
            }
            for (QueueData queueData : queueDataList) {
                String brokerName = queueData.getBrokerName();
                int readQueueNums = queueData.getReadQueueNums();
                int writeQueueNums = queueData.getWriteQueueNums();
                int perm = queueData.getPerm();
                if ((readQueueNums > 0 && PermName.isReadable(perm)) || (writeQueueNums > 0 && PermName.isWriteable(perm))) {
                    activeBrokerNameSet.add(brokerName);
                    activeTopicSet.add(topic);
                }
            }
        }

        final HeartbeatData heartbeatData = this.prepareHeartbeatData(activeTopicSet);
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            LOG.warn("sending heartbeat, but no consumer and no producer");
            return;
        }

        if (this.brokerAddrTable.isEmpty()) {
            return;
        }

        long times = this.sendHeartbeatTimesTotal.getAndIncrement();
        for (String brokerName : activeBrokerNameSet) {
            HashMap<Long, String> brokerIdAddressTable = brokerAddrTable.get(brokerName);
            if (null == brokerIdAddressTable || brokerIdAddressTable.isEmpty()) {
                continue;
            }
            for (Entry<Long, String> entry : brokerIdAddressTable.entrySet()) {
                Long id = entry.getKey();
                String addr = entry.getValue();
                if (null == addr) {
                    continue;
                }

                if (consumerEmpty) {
                    if (id != MixAll.MASTER_ID) {
                        continue;
                    }
                }

                try {
                    int version = this.mQClientAPIImpl.sendHeartbeat(addr, heartbeatData, 3000);
                    if (!this.brokerVersionTable.containsKey(brokerName)) {
                        this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                    }
                    this.brokerVersionTable.get(brokerName).put(addr, version);
                    if (times % 20 == 0) {
                        LOG.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                        LOG.info(heartbeatData.toString());
                    }
                } catch (Exception e) {
                    if (this.isBrokerInNameServer(addr)) {
                        LOG.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr);
                    } else {
                        LOG.info("send heart beat to broker[{} {} {}] exception, because the broker not " +
                                "up, forget it", brokerName,
                            id, addr);
                    }
                }

            }
        }
    }

    private void uploadFilterClassSource() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> next = it.next();
            MQConsumerInner consumer = next.getValue();
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                for (SubscriptionData sub : subscriptions) {
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            LOG.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
        DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    if (isDefault && defaultMQProducer != null) {
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                            1000 * 3);
                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        if (!changed) {
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            LOG.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        if (changed) {
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update Pub info
                            {
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                if (publishInfo.isHAOrderTopic()) {
                                    publishInfo.setQueueListMap(OrderMessageHandler.generateQueueListMap(publishInfo));
                                }
                                publishInfo.setHaveTopicRouterInfo(true);
                                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQProducerInner> entry = it.next();
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // Update sub info
                            {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQConsumerInner> entry = it.next();
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            LOG.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        LOG.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (Exception e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(MixAll.DEFAULT_TOPIC)) {
                        LOG.warn("updateTopicRouteInfoFromNameServer Exception", e);
                        if (e instanceof MQClientException && ResponseCode.TOPIC_NOT_EXIST == ((MQClientException) e).getResponseCode()) {
                            // clean no used topic
                            cleanNoneRouteTopic(topic);
                        }
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                LOG.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOG.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    private void cleanNoneRouteTopic(String topic) {
        // clean no used topic
        TopicRouteData prev = this.topicRouteTable.remove(topic);
        if (prev != null) {
            LOG.info("cleanNoneRouteTopic remove topic route data, {}, {}", topic, prev);
        }

        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    impl.removeTopicPublishInfo(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    impl.removeTopicSubscribeInfo(topic);
                }
            }
        }

        {
            this.defaultMQProducer.getDefaultMQProducerImpl().removeTopicPublishInfo(topic);
        }
    }

    private HeartbeatData prepareHeartbeatData(Set<String> activeTopicSet) {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID
        heartbeatData.setClientID(this.clientId);

        if (this.getClientConfig().isAllClientsOffline()) {
            return heartbeatData;
        }

        // Consumer
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null && !impl.isOffline()) {
                Set<SubscriptionData> subscriptions = impl.subscriptions();
                if (null == subscriptions) {
                    continue;
                }

                boolean subscriptionsIsAvailable = false;
                for (SubscriptionData subscriptionData : subscriptions) {
                    if (null == subscriptionData) {
                        continue;
                    }
                    String topic = subscriptionData.getTopic();
                    if (activeTopicSet.contains(topic)) {
                        subscriptionsIsAvailable = true;
                        break;
                    }
                }
                if (!subscriptionsIsAvailable) {
                    continue;
                }

                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(subscriptions);
                consumerData.setUnitMode(impl.isUnitMode());
                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // Producer
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null && !impl.isOffline()) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());
                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> itNext = it.next();
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain) {
                    return true;
                }
            }
        }

        return false;
    }

    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName,
        final String topic,
        final String filterClassSource) throws UnsupportedEncodingException {
        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            LOG.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                fullClassName,
                RemotingHelper.exceptionSimpleDesc(e1));
        }

        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null
            && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<String>> next = it.next();
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                            5000);

                        LOG.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                            topic, fullClassName);

                    } catch (Exception e) {
                        LOG.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            LOG.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                consumerGroup, topic, fullClassName);
        }
    }

    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null) {
            return true;
        }
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty()) {
            return;
        }

        // AdminExt
        if (!this.adminExtTable.isEmpty()) {
            return;
        }

        // Producer
        if (this.producerTable.size() > 1) {
            return;
        }

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();

                    if (this.datagramSocket != null) {
                        this.datagramSocket.close();
                        this.datagramSocket = null;
                    }
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    LOG.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            LOG.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }


    public void offlineConsumer(final String group) {
        this.unregisterClientWithLock(null, group);
    }


    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            if (this.lockHeartbeat.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    LOG.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                LOG.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            LOG.warn("unregisterClientWithLock exception", e);
        }
    }

    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    String addr = entry1.getValue();
                    if (addr != null) {
                        try {
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            LOG.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, entry1.getKey(), addr);
                        } catch (RemotingException e) {
                            LOG.warn("unregister client RemotingException from broker: {}, {}", addr, e.getMessage());
                        } catch (InterruptedException e) {
                            LOG.warn("unregister client InterruptedException from broker: {}, {}", addr, e.getMessage());
                        } catch (MQBrokerException e) {
                            LOG.warn("unregister client MQBrokerException from broker: {}, {}", addr, e.getMessage());
                        }
                    }
                }
            }
        }
    }

    public boolean registerProducer(final String group, final MQProducerInner producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            LOG.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }


    public void offlineProducer(final String group) {
        this.unregisterClientWithLock(group, null);
    }


    public void allClientsOffline() {
        this.getClientConfig().setAllClientsOffline(true);

        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                String producerName = entry.getKey();
                MQProducerInner producerInner = entry.getValue();
                if (producerName != null && !producerName.equals("")) {
                    producerInner.offline();
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                String consumerName = entry.getKey();
                if (consumerName != null && !consumerName.equals("")) {
                    MQConsumerInner consumerInner = entry.getValue();
                    if (null != consumerInner) {
                        consumerInner.offline();
                    }
                }
            }
        }
    }

    public void allClientsOnline() {
        this.getClientConfig().setAllClientsOffline(false);

        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                String producerName = entry.getKey();
                MQProducerInner producerInner = entry.getValue();
                if (producerName != null && !producerName.equals("")) {
                    producerInner.online();
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                String consumerName = entry.getKey();
                if (consumerName != null && !consumerName.equals("")) {
                    MQConsumerInner consumerInner = entry.getValue();
                    if (null != consumerInner) {
                        consumerInner.online();
                    }
                }
            }
        }
    }

    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            LOG.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    public void rebalanceLater(long delayMillis) {
        if (delayMillis <= 0) {
            this.rebalanceService.wakeup();
        } else {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    MQClientInstance.this.rebalanceService.wakeup();
                }
            }, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    public boolean doRebalance() {
        boolean balanced = true;
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (!impl.doRebalance()) {
                        balanced = false;
                    }
                } catch (Throwable e) {
                    LOG.error("doRebalance exception", e);
                }
            }
        }

        return balanced;
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                    } else {
                        slave = true;
                    }
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    public FindBrokerResult findBrokerAddressInSubscribe(
        final String brokerName,
        final long brokerId,
        final boolean onlyThisBroker
    ) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        return 0;
    }

    public List<String> findConsumerIdList(final String topic, final String group) {
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                LOG.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }

    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                LOG.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();

            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
            }

            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        LOG.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public ConcurrentMap<String, ConcurrentMap<String, Long>> getTopicQueueOffsetTable() {
        return topicQueueOffsetTable;
    }

    public ConcurrentMap<String, Long> findQueueOffsetTable(String topic) {
        ConcurrentMap<String, Long> offsetTable = this.topicQueueOffsetTable.get(topic);
        return offsetTable;
    }

    public void putQueueOffset(String topic, String key, long newOffset) {
        ConcurrentMap<String, Long> offsetTable = this.findQueueOffsetTable(topic);
        if (offsetTable == null) {
            this.topicQueueOffsetTable.putIfAbsent(topic, new ConcurrentHashMap<String, Long>());
        }
        offsetTable = this.findQueueOffsetTable(topic);

        // No thread will remove offsetTable, thus it's not necessary to check the null value.
        Long oldOffset;
        do {
            oldOffset = offsetTable.putIfAbsent(key, newOffset);
            if (oldOffset == null) {
                break;
            }
        }
        while (oldOffset < newOffset && !offsetTable.replace(key, oldOffset, newOffset));
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg,
        final String consumerGroup,
        final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public ConcurrentMap<String, MQProducerInner> getProducerTable() {
        return producerTable;
    }

    public ConcurrentMap<String, MQConsumerInner> getConsumerTable() {
        return consumerTable;
    }
}
