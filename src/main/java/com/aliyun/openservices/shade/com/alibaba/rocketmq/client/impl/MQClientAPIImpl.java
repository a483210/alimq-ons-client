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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl;

import com.alibaba.ons.open.trace.core.utils.JsonUtils;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.ClientConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.RemoteClientConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.*;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.SendMessageContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.PullResultExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendCallback;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.stat.ConsumerStatsManager;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.*;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.*;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics.rpc.RpcLabels;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.metrics.rpc.RpcMetricHookImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.namesrv.DefaultTopAddressing;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.namesrv.NameServerUpdateCallback;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.namesrv.TopAddressing;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.RequestCode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.body.*;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.*;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.filtersrv.RegisterMessageFilterClassRequestHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.header.namesrv.*;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.route.TopicRouteDatas;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.InvokeCallback;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RPCHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RemotingClient;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.*;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.netty.ResponseFuture;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.LanguageCode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.google.common.base.Optional;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MQClientAPIImpl implements NameServerUpdateCallback {
    private final static InternalLogger log = ClientLogger.getLog();
    private final static boolean SEND_SMART_MSG =
            Boolean.parseBoolean(System.getProperty("com.aliyun.openservices.shade.com.alibaba.rocketmq.client.sendSmartMsg", "true"));

    static {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
    }

    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing;
    private final ClientRemotingProcessor clientRemotingProcessor;
    private String nameSrvAddr = null;
    private ClientConfig clientConfig;
    private ConsumerStatsManager consumerStatsManager;

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
                           final ClientRemotingProcessor clientRemotingProcessor,
                           RPCHook rpcHook, final ClientConfig clientConfig) {
        this(nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig, null, null);
    }

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
                           final ClientRemotingProcessor clientRemotingProcessor,
                           RPCHook rpcHook, final ClientConfig clientConfig,
                           final EventLoopGroup eventLoopGroup, final EventExecutorGroup eventExecutorGroup) {
        this.clientConfig = clientConfig;
        if (null != clientConfig.getUnitPara() && clientConfig.getUnitPara().size() > 0) {
            topAddressing = new DefaultTopAddressing(clientConfig.getUnitName(), clientConfig.getUnitPara(), MixAll.getWSAddr());
        } else {
            topAddressing = new DefaultTopAddressing(MixAll.getWSAddr(), clientConfig.getUnitName());
        }
        topAddressing.registerChangeCallBack(this);
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, null, eventLoopGroup, eventExecutorGroup);
        this.clientRemotingProcessor = clientRemotingProcessor;

        this.remotingClient.registerRPCHook(rpcHook);
        this.remotingClient.registerRPCHook(new RpcMetricHookImpl());

        this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.PULL_METRIC_DATA, this.clientRemotingProcessor, null);
    }

    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old=" + this.nameSrvAddr + ", new=" + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    @Override
    public String onNameServerAddressChange(String namesrvAddress) {
        if (namesrvAddress != null) {
            if (!namesrvAddress.equals(this.nameSrvAddr)) {
                log.info("name server address changed, old=" + this.nameSrvAddr + ", new=" + namesrvAddress);
                this.updateNameServerAddressList(namesrvAddress);
                this.nameSrvAddr = namesrvAddress;
                return nameSrvAddr;
            }
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        String[] addrArray = addrs.split(";");
        List<String> list = Arrays.asList(addrArray);
        this.remotingClient.updateNameServerAddressList(list);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }

    public void createSubscriptionGroup(final String addr, final SubscriptionGroupConfig config,
                                        final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);

        byte[] body = JsonUtils.encode(config);
        request.setBody(body);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void createSubscriptionGroupInitOffset(final String addr, final SubscriptionGroupConfig config,
                                                  final String topic, final int initMode, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        InitConsumerOffsetRequestHeader requestHeader = new InitConsumerOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setInitMode(initMode);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUB_INIT_OFFSET, requestHeader);

        byte[] body = JsonUtils.encode(config);
        request.setBody(body);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
                            final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        createTopic(addr, defaultTopic, topicConfig, null, timeoutMillis);
    }

    public void createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
                            final QueueGroupConfig queueGroupConfig, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
        if (queueGroupConfig != null) {
            CreateOrUpdateTopicBody body = new CreateOrUpdateTopicBody(queueGroupConfig);
            request.setBody(body.encode());
        }
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void createTopicList(final String addr, final List<TopicConfig> topicConfigList,
                                final List<QueueGroupConfig> queueGroupConfigList, final long timeoutMillis)
            throws InterruptedException, RemotingException, MQClientException {
        CreateTopicListRequestHeader requestHeader = new CreateTopicListRequestHeader();
        CreateTopicListRequestBody requestBody = new CreateTopicListRequestBody(topicConfigList, queueGroupConfigList);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST, requestHeader);
        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(
                MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public SendResult sendMessage(
            final String addr, final String brokerName, final Message msg,
            final SendMessageRequestHeader requestHeader, final long timeoutMillis,
            final CommunicationMode communicationMode, final SendMessageContext context,
            final DefaultMQProducerImpl producer
    ) throws RemotingException, MQBrokerException, InterruptedException {
        return sendMessage(addr, brokerName, msg, requestHeader, timeoutMillis, communicationMode, null, null, null, 0, context, producer);
    }

    public SendResult sendMessage(
            final String addr, final String brokerName, final Message msg,
            final SendMessageRequestHeader requestHeader, final long timeoutMillis,
            final CommunicationMode communicationMode,
            final SendCallback sendCallback, final TopicPublishInfo topicPublishInfo, final MQClientInstance instance,
            final int retryTimesWhenSendFailed, final SendMessageContext context, final DefaultMQProducerImpl producer
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = null;
        if (SEND_SMART_MSG || msg instanceof MessageBatch) {
            SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
            request = RemotingCommand.createRequestCommand(msg instanceof MessageBatch ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
        } else {
            request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        }

        request.setBody(msg.getBody());

        switch (communicationMode) {
            case ONEWAY:
                RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance,
                        retryTimesWhenSendFailed, times, context, producer);
                return null;
            case SYNC:
                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis, request);
            default:
                assert false;
                break;
        }

        return null;
    }

    private SendResult sendMessageSync(
            final String addr, final String brokerName, final Message msg,
            final long timeoutMillis, final RemotingCommand request
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processSendResponse(brokerName, msg, response);
    }

    void execRpcHooksAfterRequest(ResponseFuture responseFuture) {
        if (this.remotingClient instanceof NettyRemotingClient) {
            NettyRemotingClient remotingClient = (NettyRemotingClient) this.remotingClient;
            RemotingCommand response = responseFuture.getResponseCommand();
            if (response == null) {
                remotingClient.doAfterRpcFailure(responseFuture.getRequestCommand(), responseFuture.getChannel(),
                        responseFuture.isTimeout());
            } else {
                remotingClient.doAfterResponses(responseFuture.getRequestCommand(), responseFuture.getChannel(),
                        response);
            }
        }
    }

    private void sendMessageAsync(
            final String addr, final String brokerName, final Message msg,
            final long timeoutMillis, final RemotingCommand request, final SendCallback sendCallback,
            final TopicPublishInfo topicPublishInfo, final MQClientInstance instance, final int retryTimesWhenSendFailed,
            final AtomicInteger times, final SendMessageContext context, final DefaultMQProducerImpl producer
    ) throws InterruptedException, RemotingException {
        try {
            RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
            this.remotingClient.invokeAsync(addr, request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
                @Override
                public void onComplete(ResponseFuture responseFuture) {
                    RemotingCommand response = responseFuture.getResponseCommand();
                    if (null == sendCallback && response != null) {

                        try {
                            SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response);
                            if (context != null && sendResult != null) {
                                context.setSendResult(sendResult);
                                context.getProducer().executeSendMessageHookAfter(context);
                            }
                        } catch (Throwable e) {
                        }

                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false, true);
                        return;
                    }

                    if (response != null) {
                        try {
                            SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response);
                            assert sendResult != null;
                            if (context != null) {
                                context.setSendResult(sendResult);
                                context.getProducer().executeSendMessageHookAfter(context);
                            }

                            try {
                                sendCallback.onSuccess(sendResult);
                            } catch (Throwable e) {
                            }

                            producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false, true);
                        } catch (Exception e) {
                            producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), true, true);
                            onExceptionImpl(brokerName, msg, 0L, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, e, context, true, producer);
                        }
                    } else {
                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), true, true);
                        if (!responseFuture.isSendRequestOK()) {
                            MQClientException ex = new MQClientException("send request failed", responseFuture.getCause());
                            onExceptionImpl(brokerName, msg, 0L, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                        } else if (responseFuture.isTimeout()) {
                            MQClientException ex = new MQClientException("wait response timeout " + responseFuture.getTimeoutMillis() + "ms",
                                    responseFuture.getCause());
                            onExceptionImpl(brokerName, msg, 0L, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                        } else {
                            MQClientException ex = new MQClientException("unknow reseaon", responseFuture.getCause());
                            onExceptionImpl(brokerName, msg, 0L, request, sendCallback, topicPublishInfo, instance,
                                    retryTimesWhenSendFailed, times, ex, context, true, producer);
                        }
                    }
                }
            });
        } catch (Exception e) {
            onExceptionImpl(brokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance,
                    retryTimesWhenSendFailed, times, e, context, true, producer);
        }
    }

    private void onExceptionImpl(final String brokerName,
                                 final Message msg, final long timeoutMillis, final RemotingCommand request,
                                 final SendCallback sendCallback, final TopicPublishInfo topicPublishInfo, final MQClientInstance instance,
                                 final int timesTotal, final AtomicInteger curTimes, final Exception e, final SendMessageContext context,
                                 final boolean needRetry, final DefaultMQProducerImpl producer
    ) {
        int tmp = curTimes.incrementAndGet();
        if (needRetry && tmp <= timesTotal) {
            String retryBrokerName = brokerName;//by default, it will send to the same broker
            if (topicPublishInfo != null) { //select one message queue accordingly, in order to determine which broker to send
                MessageQueue mqChosen = producer.selectOneMessageQueue(topicPublishInfo, brokerName, false);
                retryBrokerName = mqChosen.getBrokerName();
            }
            String addr = instance.findBrokerAddressInPublish(retryBrokerName);
            log.info("async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}", tmp, msg.getTopic(), addr,
                    retryBrokerName);
            try {
                request.setOpaque(RemotingCommand.createNewRequestId());
                sendMessageAsync(addr, retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance,
                        timesTotal, curTimes, context, producer);
            } catch (InterruptedException e1) {
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, false, producer);
            } catch (RemotingConnectException e1) {
                producer.updateFaultItem(brokerName, 0, true, false);
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, true, producer);
            } catch (RemotingTooMuchRequestException e1) {
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, false, producer);
            } catch (RemotingException e1) {
                producer.updateFaultItem(brokerName, 0, true, false);
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, true, producer);
            }
        } else {

            if (context != null) {
                context.setException(e);
                context.getProducer().executeSendMessageHookAfter(context);
            }

            try {
                sendCallback.onException(e);
            } catch (Exception ignored) {
            }
        }
    }

    private SendResult processSendResponse(final String brokerName, final Message msg, final RemotingCommand response)
            throws MQBrokerException, RemotingCommandException {
        switch (response.getCode()) {
            case ResponseCode.FLUSH_DISK_TIMEOUT:
            case ResponseCode.FLUSH_SLAVE_TIMEOUT:
            case ResponseCode.SLAVE_NOT_AVAILABLE: {
            }
            case ResponseCode.SUCCESS: {
                SendStatus sendStatus = SendStatus.SEND_OK;
                switch (response.getCode()) {
                    case ResponseCode.FLUSH_DISK_TIMEOUT:
                        sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                        break;
                    case ResponseCode.FLUSH_SLAVE_TIMEOUT:
                        sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                        break;
                    case ResponseCode.SLAVE_NOT_AVAILABLE:
                        sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                        break;
                    case ResponseCode.SUCCESS:
                        sendStatus = SendStatus.SEND_OK;
                        break;
                    default:
                        assert false;
                        break;
                }

                SendMessageResponseHeader responseHeader =
                        (SendMessageResponseHeader) response.decodeCommandCustomHeader(SendMessageResponseHeader.class);

                //If namespace not null , reset Topic without namespace.
                String topic = msg.getTopic();
                if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
                    topic = NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace());
                }

                MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());

                String uniqMsgId = MessageClientIDSetter.getUniqID(msg);
                if (msg instanceof MessageBatch) {
                    StringBuilder sb = new StringBuilder();
                    for (Message message : (MessageBatch) msg) {
                        sb.append(sb.length() == 0 ? "" : ",").append(MessageClientIDSetter.getUniqID(message));
                    }
                    uniqMsgId = sb.toString();
                }
                SendResult sendResult = new SendResult(sendStatus,
                        uniqMsgId,
                        responseHeader.getMsgId(), messageQueue, responseHeader.getQueueOffset());
                sendResult.setTransactionId(responseHeader.getTransactionId());
                String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
                String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE_SWITCH);
                if (regionId == null || regionId.isEmpty()) {
                    regionId = MixAll.DEFAULT_TRACE_REGION_ID;
                }
                if (traceOn != null && traceOn.equals("false")) {
                    sendResult.setTraceOn(false);
                } else {
                    sendResult.setTraceOn(true);
                }
                sendResult.setRegionId(regionId);
                return sendResult;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public PullResult pullMessage(
            final String addr, final PullMessageRequestHeader requestHeader, final long timeoutMillis,
            final CommunicationMode communicationMode, final PullCallback pullCallback
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request;
        if (PullSysFlag.hasLitePullFlag(requestHeader.getSysFlag())) {
            request = RemotingCommand.createRequestCommand(RequestCode.LITE_PULL_MESSAGE, requestHeader);
        } else {
            request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
        }

        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.pullMessageSync(addr, request, timeoutMillis);
            default:
                assert false;
                break;
        }

        return null;
    }

    public PopResult popMessage(
            final String brokerName, final String addr, final PopMessageRequestHeader requestHeader,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPopResponse(brokerName, response, requestHeader.getTopic(), requestHeader);
    }

    public void popMessageAsync(
            final String brokerName, final String addr, final PopMessageRequestHeader requestHeader,
            final long timeoutMillis, final PopCallback popCallback
    ) throws RemotingException, InterruptedException {
        incPopQPS(addr, requestHeader);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
            @Override
            public void onComplete(ResponseFuture responseFuture) {
                incPopRT(addr, requestHeader, responseFuture);
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        PopResult popResult = MQClientAPIImpl.this.processPopResponse(brokerName, response, requestHeader.getTopic(), requestHeader);
                        assert popResult != null;
                        popCallback.onSuccess(popResult);
                    } catch (Exception e) {
                        popCallback.onException(e);
                    }
                } else {
                    if (!responseFuture.isSendRequestOK()) {
                        popCallback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        popCallback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        popCallback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                    }
                }
            }
        });
    }

    public void notificationAsync(
            final String brokerName, final String addr, final NotificationRequestHeader requestHeader,
            final long timeoutMillis, final NotificationCallback callback
    ) throws RemotingException, InterruptedException {
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFICATION, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
            @Override
            public void onComplete(ResponseFuture responseFuture) {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        NotificationResponseHeader responseHeader = (NotificationResponseHeader) response.decodeCommandCustomHeader(NotificationResponseHeader.class);
                        callback.onSuccess(responseHeader.isHasMsg());
                    } catch (Exception e) {
                        callback.onException(e);
                    }
                } else {
                    if (!responseFuture.isSendRequestOK()) {
                        callback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        callback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        callback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                    }
                }
            }
        });
    }

    public void pollingInfoAsync(
            final String brokerName,
            final String addr, //
            final PollingInfoRequestHeader requestHeader, //
            final long timeoutMillis,//
            final PollingInfoCallback callback//
    ) throws RemotingException, InterruptedException {
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POLLING_INFO, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
            @Override
            public void onComplete(ResponseFuture responseFuture) {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        PollingInfoResponseHeader responseHeader = (PollingInfoResponseHeader) response.decodeCommandCustomHeader(PollingInfoResponseHeader.class);
                        callback.onSuccess(responseHeader.getPollingNum());
                    } catch (Exception e) {
                        callback.onException(e);
                    }
                } else {
                    if (!responseFuture.isSendRequestOK()) {
                        callback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        callback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        callback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                    }
                }
            }
        });
    }

    public void peekMessageAsync(//
                                 final String brokerName,
                                 final String addr, //
                                 final PeekMessageRequestHeader requestHeader, //
                                 final long timeoutMillis,//
                                 final PopCallback popCallback//
    ) throws RemotingException, InterruptedException {
        incPeekQPS(addr, requestHeader);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PEEK_MESSAGE, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
            @Override
            public void onComplete(ResponseFuture responseFuture) {
                incPeekRT(addr, requestHeader, responseFuture);
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        PopResult popResult = MQClientAPIImpl.this.processPopResponse(brokerName, response, requestHeader.getTopic(), requestHeader);
                        assert popResult != null;
                        popCallback.onSuccess(popResult);
                    } catch (Exception e) {
                        popCallback.onException(e);
                    }
                } else {
                    if (!responseFuture.isSendRequestOK()) {
                        popCallback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        popCallback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        popCallback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                    }
                }
            }
        });
    }

    public PopResult peekMessage(//
                                 final String brokerName,
                                 final String addr, //
                                 final PeekMessageRequestHeader requestHeader, //
                                 final long timeoutMillis //
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PEEK_MESSAGE, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPopResponse(brokerName, response, requestHeader.getTopic(), requestHeader);
    }

    public void ackMessage(//
                           final String addr, //
                           final AckMessageRequestHeader requestHeader //
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeOneway(addr, request, 1000L);
    }

    public void ackMessageAsync(
            final String addr,
            final long timeOut,
            final AckCallback ackCallback,
            final AckMessageRequestHeader requestHeader //
    ) throws RemotingException, MQBrokerException, InterruptedException {
        incAckQPS(addr, requestHeader);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(addr, request, timeOut, new BaseInvokeCallback(MQClientAPIImpl.this) {

            @Override
            public void onComplete(ResponseFuture responseFuture) {
                incAckRT(addr, requestHeader, responseFuture);
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        AckResult ackResult = new AckResult();
                        ackResult.setResponseCode(response.getCode());
                        ackResult.setResponseRemark(response.getRemark());
                        if (ResponseCode.SUCCESS == response.getCode()) {
                            ackResult.setStatus(AckStatus.OK);
                        } else {
                            ackResult.setStatus(AckStatus.NO_EXIST);
                        }
                        assert ackResult != null;
                        ackCallback.onSuccess(ackResult);
                    } catch (Exception e) {
                        ackCallback.onException(e);
                    }
                } else {
                    if (!responseFuture.isSendRequestOK()) {
                        ackCallback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        ackCallback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        ackCallback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeOut + ". Request: " + request, responseFuture.getCause()));
                    }
                }

            }
        });
    }

    public void changeInvisibleTimeAsync(//
                                         final String brokerName,
                                         final String addr, //
                                         final ChangeInvisibleTimeRequestHeader requestHeader,//
                                         final long timeoutMillis,
                                         final AckCallback ackCallback
    ) throws RemotingException, MQBrokerException, InterruptedException {
        incChangeQPS(addr, requestHeader);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
            @Override
            public void onComplete(ResponseFuture responseFuture) {
                incChangeRT(addr, requestHeader, responseFuture);
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) response.decodeCommandCustomHeader(ChangeInvisibleTimeResponseHeader.class);
                        AckResult ackResult = new AckResult();
                        ackResult.setResponseCode(response.getCode());
                        ackResult.setResponseRemark(response.getRemark());
                        if (ResponseCode.SUCCESS == response.getCode()) {
                            ackResult.setStatus(AckStatus.OK);
                            ackResult.setPopTime(responseHeader.getPopTime());
                            ackResult.setExtraInfo(ExtraInfoUtil.buildExtraInfo(requestHeader.getOffset(), responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                                    responseHeader.getReviveQid(), requestHeader.getTopic(), brokerName, requestHeader.getQueueId()) + MessageConst.KEY_SEPARATOR
                                    + requestHeader.getOffset());
                        } else {
                            ackResult.setStatus(AckStatus.NO_EXIST);
                        }
                        assert ackResult != null;
                        ackCallback.onSuccess(ackResult);
                    } catch (Exception e) {
                        ackCallback.onException(e);
                    }
                } else {
                    if (!responseFuture.isSendRequestOK()) {
                        ackCallback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        ackCallback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        ackCallback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                    }
                }
            }
        });
    }

    private void pullMessageAsync(
            final String addr,
            final RemotingCommand request,
            final long timeoutMillis,
            final PullCallback pullCallback
    ) throws RemotingException, InterruptedException {
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
            @Override
            public void onComplete(ResponseFuture responseFuture) {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        PullResult pullResult = MQClientAPIImpl.this.processPullResponse(response);
                        assert pullResult != null;
                        pullCallback.onSuccess(pullResult);
                    } catch (Exception e) {
                        pullCallback.onException(e);
                    }
                } else {
                    if (!responseFuture.isSendRequestOK()) {
                        pullCallback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        pullCallback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        pullCallback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                    }
                }
            }
        });
    }

    private PullResult pullMessageSync(
            final String addr,
            final RemotingCommand request,
            final long timeoutMillis
    ) throws RemotingException, InterruptedException, MQBrokerException {
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPullResponse(response);
    }

    private PullResult processPullResponse(
            final RemotingCommand response) throws MQBrokerException, RemotingCommandException {
        PullStatus pullStatus = PullStatus.NO_NEW_MSG;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                pullStatus = PullStatus.FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                pullStatus = PullStatus.NO_NEW_MSG;
                break;
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                pullStatus = PullStatus.NO_MATCHED_MSG;
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                break;

            default:
                throw new MQBrokerException(response.getCode(), response.getRemark());
        }

        PullMessageResponseHeader responseHeader =
                (PullMessageResponseHeader) response.decodeCommandCustomHeader(PullMessageResponseHeader.class);
        return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(),
                responseHeader.getMaxOffset(), null, responseHeader.getSuggestWhichBrokerId(), response.getBody());
    }

    private PopResult processPopResponse(final String brokerName, final RemotingCommand response, String topic,
                                         CommandCustomHeader requestHeader) throws MQBrokerException, RemotingCommandException {
        PopStatus popStatus = PopStatus.NO_NEW_MSG;
        List<MessageExt> msgFoundList = null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                popStatus = PopStatus.FOUND;
                ByteBuffer byteBuffer = response.getBody();
                msgFoundList = MessageDecoder.decodes(byteBuffer);
                break;
            case ResponseCode.POLLING_FULL:
                popStatus = PopStatus.POLLING_FULL;
                break;
            case ResponseCode.POLLING_TIMEOUT:
                popStatus = PopStatus.POLLING_NOT_FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                popStatus = PopStatus.POLLING_NOT_FOUND;
                break;
            default:
                throw new MQBrokerException(response.getCode(), response.getRemark());
        }

        PopResult popResult = new PopResult(popStatus, msgFoundList);
        PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.decodeCommandCustomHeader(PopMessageResponseHeader.class);
        popResult.setRestNum(responseHeader.getRestNum());
        // it is a pop command if pop time greater than 0, we should set the check point info to extraInfo field
        if (popStatus == PopStatus.FOUND) {
            Map<String, Long> startOffsetInfo = null;
            Map<String, List<Long>> msgOffsetInfo = null;
            Map<String, Integer> orderCountInfo = null;
            if (requestHeader instanceof PopMessageRequestHeader) {
                popResult.setInvisibleTime(responseHeader.getInvisibleTime());
                popResult.setPopTime(responseHeader.getPopTime());
                startOffsetInfo = ExtraInfoUtil.parseStartOffsetInfo(responseHeader.getStartOffsetInfo());
                msgOffsetInfo = ExtraInfoUtil.parseMsgOffsetInfo(responseHeader.getMsgOffsetInfo());
                orderCountInfo = ExtraInfoUtil.parseOrderCountInfo(responseHeader.getOrderCountInfo());
            }
            Map<String/*topicMark@queueId*/, List<Long>/*msg queueOffset*/> sortMap = new HashMap<String, List<Long>>(16);
            for (MessageExt messageExt : msgFoundList) {
                String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getQueueId());
                if (!sortMap.containsKey(key)) {
                    sortMap.put(key, new ArrayList<Long>(4));
                }
                sortMap.get(key).add(messageExt.getQueueOffset());
            }
            Map<String, String> map = new HashMap<String, String>(5);
            for (MessageExt messageExt : msgFoundList) {
                if (requestHeader instanceof PopMessageRequestHeader) {
                    if (startOffsetInfo == null) {
                        // we should set the check point info to extraInfo field , if the command is popMsg
                        // find pop ck offset
                        String key = messageExt.getTopic() + messageExt.getQueueId();
                        if (!map.containsKey(messageExt.getTopic() + messageExt.getQueueId())) {
                            map.put(key, ExtraInfoUtil.buildExtraInfo(messageExt.getQueueOffset(), responseHeader.getPopTime(), responseHeader.getInvisibleTime(), responseHeader.getReviveQid(),
                                    messageExt.getTopic(), brokerName, messageExt.getQueueId()));

                        }
                        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, map.get(key) + MessageConst.KEY_SEPARATOR + messageExt.getQueueOffset());
                    } else {
                        String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getQueueId());
                        int index = sortMap.get(key).indexOf(messageExt.getQueueOffset());
                        Long msgQueueOffset = msgOffsetInfo.get(key).get(index);
                        if (msgQueueOffset != messageExt.getQueueOffset()) {
                            log.warn("Queue offset[%d] of msg is strange, not equal to the stored in msg, %s", msgQueueOffset, messageExt);
                        }

                        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK,
                                ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(key).longValue(), responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                                        responseHeader.getReviveQid(), messageExt.getTopic(), brokerName, messageExt.getQueueId(), msgQueueOffset.longValue())
                        );
                        if (((PopMessageRequestHeader) requestHeader).isOrder() && orderCountInfo != null) {
                            Integer count = orderCountInfo.get(key);
                            if (count != null && count > 0) {
                                messageExt.setReconsumeTimes(count);
                            }
                        }
                    }
                    if (messageExt.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
                        messageExt.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(responseHeader.getPopTime()));
                    }
                }
                messageExt.setTopic(NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace()));
            }
        }
        return popResult;
    }

    public MessageExt viewMessage(final String addr, final long phyoffset, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
        requestHeader.setOffset(phyoffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ByteBuffer byteBuffer = response.getBody();
                MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
                //If namespace not null , reset Topic without namespace.
                if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
                    messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.clientConfig.getNamespace()));
                }
                return messageExt;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long searchOffset(final String addr, final String topic, final int queueId, final long timestamp,
                             final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        return searchOffset(addr, topic, queueId, timestamp, timeoutMillis, BoundaryType.LOWER);
    }

    public long searchOffset(final String addr, final String topic, final int queueId, final long timestamp,
                             final long timeoutMillis, BoundaryType boundaryType)
            throws RemotingException, MQBrokerException, InterruptedException {
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setBoundary(boundaryType.getName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                SearchOffsetResponseHeader responseHeader =
                        (SearchOffsetResponseHeader) response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long getMaxOffset(final String addr, final String topic, final int queueId, final boolean committed,
                             final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setCommitted(committed);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMaxOffsetResponseHeader responseHeader =
                        (GetMaxOffsetResponseHeader) response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public List<String> getConsumerIdListByGroup(
            final String addr,
            final String consumerGroup,
            final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            MQBrokerException, InterruptedException {
        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    GetConsumerListByGroupResponseBody body = JsonUtils.decode(response.getBody(), GetConsumerListByGroupResponseBody.class);
                    return body.getConsumerIdList();
                }
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long getMinOffset(final String addr, final String topic, final int queueId, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMinOffsetResponseHeader responseHeader =
                        (GetMinOffsetResponseHeader) response.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long getEarliestMsgStoretime(final String addr, final String topic, final int queueId,
                                        final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetEarliestMsgStoretimeResponseHeader responseHeader =
                        (GetEarliestMsgStoretimeResponseHeader) response.decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);
                return responseHeader.getTimestamp();
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public Optional<Long> queryConsumerOffset(
            final String addr,
            final QueryConsumerOffsetRequestHeader requestHeader,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                QueryConsumerOffsetResponseHeader responseHeader =
                        response.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);
                return Optional.of(responseHeader.getOffset());
            case ResponseCode.QUERY_NOT_FOUND:
                return Optional.absent();
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public int updateConsumerOffset(
            final String addr,
            final UpdateConsumerOffsetRequestHeader requestHeader,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return response.getVersion();
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateConsumerOffsetOneway(
            final String addr,
            final UpdateConsumerOffsetRequestHeader requestHeader,
            final long timeoutMillis
    ) throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException,
            InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeOneway(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
    }

    public int sendHeartbeat(
            final String addr,
            final HeartbeatData heartbeatData,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(clientConfig.getLanguage());
        request.setBody(heartbeatData.encode());
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return response.getVersion();
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void unregisterClient(
            final String addr,
            final String clientID,
            final String producerGroup,
            final String consumerGroup,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        final UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientID);
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void endTransactionOneway(
            final String addr,
            final EndTransactionRequestHeader requestHeader,
            final String remark,
            final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);
        request.setRemark(remark);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }

    public void queryMessage(
            final String addr,
            final QueryMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final InvokeCallback invokeCallback,
            final Boolean isUnqiueKey
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, isUnqiueKey.toString());
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis,
                invokeCallback);
    }

    public boolean registerClient(final String addr, final HeartbeatData heartbeat, final long timeoutMillis)
            throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody(heartbeat.encode());
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        return response.getCode() == ResponseCode.SUCCESS;
    }

    public void consumerSendMessageBack(
            final String addr,
            final MessageExt msg,
            final String consumerGroup,
            final int delayLevel,
            final long timeoutMillis,
            final int maxConsumeRetryTimes
    ) throws RemotingException, MQBrokerException, InterruptedException {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

        requestHeader.setGroup(consumerGroup);
        requestHeader.setOriginTopic(msg.getTopic());
        requestHeader.setOffset(msg.getCommitLogOffset());
        requestHeader.setDelayLevel(delayLevel);
        requestHeader.setOriginMsgId(msg.getMsgId());
        requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);
        requestHeader.setMshaRetry(StringUtils.isNotBlank(msg.getProperty(MessageConst.PROPERTY_TRANSIENT_MSHA_RETRY)));

        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public Set<MessageQueue> lockBatchMQ(
            final String addr,
            final LockBatchRequestBody requestBody,
            final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                LockBatchResponseBody responseBody = JsonUtils.decode(response.getBody(), LockBatchResponseBody.class);
                Set<MessageQueue> messageQueues = responseBody.getLockOKMQSet();
                return messageQueues;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void lockBatchMQAsync(
            final String addr,
            final LockBatchRequestBody requestBody,
            final long timeoutMillis,
            final LockCallback callback) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
                    @Override
                    public void onComplete(ResponseFuture responseFuture) {
                        if (callback == null) {
                            return;
                        }

                        try {
                            RemotingCommand response = responseFuture.getResponseCommand();
                            if (response != null) {
                                if (response.getCode() == ResponseCode.SUCCESS) {
                                    LockBatchResponseBody responseBody = JsonUtils.decode(response.getBody(), LockBatchResponseBody.class);
                                    Set<MessageQueue> messageQueues = responseBody.getLockOKMQSet();
                                    callback.onSuccess(messageQueues);
                                } else {
                                    callback.onException(new MQBrokerException(response.getCode(), response.getRemark()));
                                }
                            }
                        } catch (Throwable e) {
                        }
                    }
                });
    }

    public void unlockBatchMQ(
            final String addr,
            final UnlockBatchRequestBody requestBody,
            final long timeoutMillis,
            final boolean oneway
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());

        if (oneway) {
            RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
        } else {
            RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
            RemotingCommand response = this.remotingClient
                    .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }
            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }

    public TopicStatsTable getTopicStatsInfo(final String addr, final String topic,
                                             final long timeoutMillis) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                TopicStatsTable topicStatsTable = JsonUtils.decode(response.getBody(), TopicStatsTable.class);
                return topicStatsTable;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQBrokerException {
        return getConsumeStats(addr, consumerGroup, null, timeoutMillis);
    }

    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final String topic,
                                        final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQBrokerException {
        GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ConsumeStats consumeStats = JsonUtils.decode(response.getBody(), ConsumeStats.class);
                return consumeStats;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ProducerConnection getProducerConnectionList(final String addr, final String producerGroup,
                                                        final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup(producerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return JsonUtils.decode(response.getBody(), ProducerConnection.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ConsumerConnection getConsumerConnectionList(final String addr, final String consumerGroup,
                                                        final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return JsonUtils.decode(response.getBody(), ConsumerConnection.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public KVTable getBrokerRuntimeInfo(final String addr, final long timeoutMillis) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return JsonUtils.decode(response.getBody(), KVTable.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateBrokerConfig(final String addr, final Properties properties, final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException, UnsupportedEncodingException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);

        String str = MixAll.properties2String(properties);
        if (str != null && str.length() > 0) {
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
            RemotingCommand response = this.remotingClient
                    .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }

    public Properties getBrokerConfig(final String addr, final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return MixAll.byteBuffer2Properties(response.getBody());
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ClusterInfo getBrokerClusterInfo(
            final long timeoutMillis) throws InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return JsonUtils.decode(response.getBody(), ClusterInfo.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {

        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, false);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {

        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
    }

    public TopicRouteDatas getTopicRouteInfoFromNameServer(final List<String> topics, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        if (topics.size() == 1) {
            TopicRouteData topicRouteData = getTopicRouteInfoFromNameServer(topics.get(0), timeoutMillis, true);
            TopicRouteDatas topicRouteDatas = new TopicRouteDatas();
            topicRouteDatas.getTopics().put(topics.get(0), topicRouteData);

            return topicRouteDatas;
        }

        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < topics.size(); i++) {
            stringBuilder.append(topics.get(i));
            if (i < topics.size() - 1) {
                stringBuilder.append(GetRouteInfoRequestHeader.split);
            }
        }
        requestHeader.setTopic(stringBuilder.toString());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    return JsonUtils.decode(response.getBody(), TopicRouteDatas.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());

    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis,
                                                          boolean allowTopicNotExist) throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist && !topic.equals(MixAll.DEFAULT_TOPIC)) {
                    log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }

                break;
            }
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    return JsonUtils.decode(response.getBody(), TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getTopicListFromNameServer(final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    return TopicList.decode(response.getBody());
                }
            }
            case ResponseCode.SYSTEM_BUSY:
                System.err.println("System is busy: " + response.getRemark());
                break;
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName,
                                     final long timeoutMillis) throws RemotingCommandException,
            RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        WipeWritePermOfBrokerRequestHeader requestHeader = new WipeWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                WipeWritePermOfBrokerResponseHeader responseHeader =
                        (WipeWritePermOfBrokerResponseHeader) response.decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
                return responseHeader.getWipeTopicCount();
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInBroker(final String addr, final String topic, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInNameServer(final String addr, final String topic, final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInNameServer(final String addr, final String clusterName, final String topic,
                                        final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        DeleteTopicInNamesrvRequestHeader requestHeader = new DeleteTopicInNamesrvRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setClusterName(clusterName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void registerTopicToNameServer(final String addr, final String topic, List<QueueData> queueDatas,
                                          final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        RegisterTopicRequestHeader requestHeader = new RegisterTopicRequestHeader();
        requestHeader.setTopic(topic);

        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setQueueDatas(queueDatas);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_TOPIC_IN_NAMESRV, requestHeader);
        request.setBody(topicRouteData.encode());
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteSubscriptionGroup(final String addr, final String groupName, final boolean cleanOffset,
                                        final long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
        requestHeader.setGroupName(groupName);
        requestHeader.setCleanOffset(cleanOffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public String getKVConfigValue(final String namespace, final String key, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetKVConfigResponseHeader responseHeader =
                        (GetKVConfigResponseHeader) response.decodeCommandCustomHeader(GetKVConfigResponseHeader.class);
                return responseHeader.getValue();
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void putKVConfigValue(final String namespace, final String key, final String value, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);
        requestHeader.setValue(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }

            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public void deleteKVConfigValue(final String namespace, final String key, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }
            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public KVTable getKVListByNamespace(final String namespace, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetKVListByNamespaceRequestHeader requestHeader = new GetKVListByNamespaceRequestHeader();
        requestHeader.setNamespace(namespace);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KVLIST_BY_NAMESPACE, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return JsonUtils.decode(response.getBody(), KVTable.class);
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffsetForSingleQueue(final String addr, final String topic,
                                                                           final String group, final long timestamp, int queueId, Long offset, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setOffset(offset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET,
                requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = remotingClient.invokeSync(
                MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (null != response.getByteArrayBody()) {
                    return JsonUtils.decode(response.getByteArrayBody(), ResetOffsetBody.class).getOffsetTable();
                }
                break;
            }
            case ResponseCode.OFFSET_TOO_SMALL:
                log.warn("Offset to reset to is too small");
                break;

            case ResponseCode.OFFSET_TOO_LARGE:
                log.warn("Offset to reset to is too large");
                break;

            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group,
                                                             final long timestamp, final boolean isForce, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        return invokeBrokerToResetOffset(addr, topic, group, timestamp, isForce, timeoutMillis, false);
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group,
                                                             final long timestamp, final boolean isForce, final long timeoutMillis, boolean isC)
            throws RemotingException, MQClientException, InterruptedException {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setForce(isForce);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        if (isC) {
            request.setLanguage(LanguageCode.CPP);
        }
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    ResetOffsetBody body = JsonUtils.decode(response.getBody(), ResetOffsetBody.class);
                    return body.getOffsetTable();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<String, Map<MessageQueue, Long>> invokeBrokerToGetConsumerStatus(final String addr, final String topic,
                                                                                final String group,
                                                                                final String clientAddr,
                                                                                final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setClientAddr(clientAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    GetConsumerStatusBody body = JsonUtils.decode(response.getBody(), GetConsumerStatusBody.class);
                    return body.getConsumerTable();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public GroupList queryTopicConsumeByWho(final String addr, final String topic, final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GroupList groupList = JsonUtils.decode(response.getBody(), GroupList.class);
                return groupList;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicList queryTopicsByConsumer(final String addr, final String group, final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        QueryTopicsByConsumerRequestHeader requestHeader = new QueryTopicsByConsumerRequestHeader();
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPICS_BY_CONSUMER, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                TopicList topicList = JsonUtils.decode(response.getBody(), TopicList.class);
                return topicList;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public SubscriptionData querySubscriptionByConsumer(final String addr, final String group, final String topic,
                                                        final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        QuerySubscriptionByConsumerRequestHeader requestHeader = new QuerySubscriptionByConsumerRequestHeader();
        requestHeader.setGroup(group);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QuerySubscriptionResponseBody subscriptionResponseBody = JsonUtils.decode(response.getBody(), QuerySubscriptionResponseBody.class);
                return subscriptionResponseBody.getSubscriptionData();
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String addr, final String topic, final String group,
                                                    final long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            MQBrokerException {
        QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumeTimeSpanBody consumeTimeSpanBody = JsonUtils.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
                return consumeTimeSpanBody.getConsumeTimeSpanSet();
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicList getTopicsByCluster(final String cluster, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
        requestHeader.setCluster(cluster);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    TopicList topicList = JsonUtils.decode(response.getBody(), TopicList.class);
                    return topicList;
                }
            }
            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void registerMessageFilterClass(final String addr,
                                           final String consumerGroup,
                                           final String topic,
                                           final String className,
                                           final int classCRC,
                                           final byte[] classBody,
                                           final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException, MQBrokerException {
        RegisterMessageFilterClassRequestHeader requestHeader = new RegisterMessageFilterClassRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClassName(className);
        requestHeader.setTopic(topic);
        requestHeader.setClassCRC(classCRC);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_MESSAGE_FILTER_CLASS, requestHeader);
        request.setBody(classBody);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicList getSystemTopicList(
            final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    TopicList topicList = JsonUtils.decode(response.getBody(), TopicList.class);
                    if (topicList.getTopicList() != null && !topicList.getTopicList().isEmpty()
                            && !UtilAll.isBlank(topicList.getBrokerAddr())) {
                        TopicList tmp = getSystemTopicListFromBroker(topicList.getBrokerAddr(), timeoutMillis);
                        if (tmp.getTopicList() != null && !tmp.getTopicList().isEmpty()) {
                            topicList.getTopicList().addAll(tmp.getTopicList());
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getSystemTopicListFromBroker(final String addr, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    TopicList topicList = JsonUtils.decode(response.getBody(), TopicList.class);
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public boolean cleanExpiredConsumeQueue(final String addr,
                                            long timeoutMillis) throws MQClientException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public boolean cleanUnusedTopicByAddr(final String addr,
                                          long timeoutMillis) throws MQClientException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_UNUSED_TOPIC, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumerRunningInfo getConsumerRunningInfo(final String addr, String consumerGroup, String clientId,
                                                      boolean jstack, final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        return this.getConsumerRunningInfo(addr, consumerGroup, clientId, jstack, false, timeoutMillis);
    }

    public ConsumerRunningInfo getConsumerRunningInfo(final String addr, String consumerGroup, String clientId,
                                                      boolean jstack, boolean metrics,
                                                      final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setJstackEnable(jstack);
        requestHeader.setMetricsEnable(metrics);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    return JsonUtils.decode(response.getBody(), ConsumerRunningInfo.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final String addr,
                                                               String consumerGroup,
                                                               String clientId,
                                                               String topic,
                                                               String uniqueKey,
                                                               String msgId,
                                                               final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setTopic(topic);
        requestHeader.setUniqueKey(uniqueKey);
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    return JsonUtils.decode(response.getBody(), ConsumeMessageDirectlyResult.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<Integer, Long> queryCorrectionOffset(final String addr, final String topic, final String group,
                                                    Set<String> filterGroup,
                                                    long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException {
        QueryCorrectionOffsetHeader requestHeader = new QueryCorrectionOffsetHeader();
        requestHeader.setCompareGroup(group);
        requestHeader.setTopic(topic);
        if (filterGroup != null) {
            StringBuilder sb = new StringBuilder();
            String splitor = "";
            for (String s : filterGroup) {
                sb.append(splitor).append(s);
                splitor = ",";
            }
            requestHeader.setFilterGroups(sb.toString());
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CORRECTION_OFFSET, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    QueryCorrectionOffsetBody body = JsonUtils.decode(response.getBody(), QueryCorrectionOffsetBody.class);
                    return body.getCorrectionOffsets();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getUnitTopicList(final boolean containRetry, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_UNIT_TOPIC_LIST, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    TopicList topicList = JsonUtils.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }

                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getHasUnitSubTopicList(final boolean containRetry, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    TopicList topicList = JsonUtils.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getHasUnitSubUnUnitTopicList(final boolean containRetry, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    TopicList topicList = JsonUtils.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))
                                it.remove();
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void cloneGroupOffset(final String addr, final String srcGroup, final String destGroup, final String topic,
                                 final boolean isOffline,
                                 final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        CloneGroupOffsetRequestHeader requestHeader = new CloneGroupOffsetRequestHeader();
        requestHeader.setSrcGroup(srcGroup);
        requestHeader.setDestGroup(destGroup);
        requestHeader.setTopic(topic);
        requestHeader.setOffline(isOffline);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLONE_GROUP_OFFSET, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public BrokerStatsData viewBrokerStatsData(String brokerAddr, String statsName, String statsKey, long timeoutMillis)
            throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException {
        ViewBrokerStatsDataRequestHeader requestHeader = new ViewBrokerStatsDataRequestHeader();
        requestHeader.setStatsName(statsName);
        requestHeader.setStatsKey(statsKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    return JsonUtils.decode(response.getBody(), BrokerStatsData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Set<String> getClusterList(String topic,
                                      long timeoutMillis) throws MQClientException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        return Collections.EMPTY_SET;
    }

    public ConsumeStatsList fetchConsumeStatsInBroker(String brokerAddr, boolean isOrder,
                                                      long timeoutMillis) throws MQClientException,
            RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        GetConsumeStatsInBrokerHeader requestHeader = new GetConsumeStatsInBrokerHeader();
        requestHeader.setIsOrder(isOrder);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONSUME_STATS, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    return JsonUtils.decode(response.getBody(), ConsumeStatsList.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr,
                                                            long timeoutMillis) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return JsonUtils.decode(response.getBody(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicConfig getTopicConfig(final String brokerAddr, String topic,
                                      long timeoutMillis) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        GetTopicConfigRequestHeader header = new GetTopicConfigRequestHeader();
        header.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, header);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return JsonUtils.decode(response.getBody(), TopicConfig.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicConfigSerializeWrapper getAllTopicConfig(final String addr,
                                                         long timeoutMillis) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return JsonUtils.decode(response.getBody(), TopicConfigSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateNameServerConfig(final Properties properties, final List<String> nameServers, long timeoutMillis)
            throws UnsupportedEncodingException,
            MQBrokerException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQClientException {
        String str = MixAll.properties2String(properties);
        if (str == null || str.length() < 1) {
            return;
        }
        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ?
                this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_NAMESRV_CONFIG, null);
        request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

        RemotingCommand errResponse = null;
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);
            assert response != null;
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    break;
                }
                default:
                    errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    public Map<String, Properties> getNameServerConfig(final List<String> nameServers, long timeoutMillis)
            throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQClientException, UnsupportedEncodingException {
        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ?
                this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_NAMESRV_CONFIG, null);

        Map<String, Properties> configMap = new HashMap<String, Properties>(4);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);

            assert response != null;

            if (ResponseCode.SUCCESS == response.getCode()) {
                configMap.put(nameServer, MixAll.byteBuffer2Properties(response.getBody()));
            } else {
                throw new MQClientException(response.getCode(), response.getRemark());
            }
        }
        return configMap;
    }

    public Properties getNameServerConfig(final String nameServer, long timeoutMillis)
            throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQClientException, UnsupportedEncodingException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_NAMESRV_CONFIG, null);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);

        assert response != null;
        Properties returnProperty;
        if (ResponseCode.SUCCESS == response.getCode()) {
            returnProperty = MixAll.byteBuffer2Properties(response.getBody());
        } else {
            throw new MQClientException(response.getCode(), response.getRemark());
        }
        return returnProperty;
    }

    public Properties getRemoteClientConfig(long timeoutMillis)
            throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
            MQClientException, UnsupportedEncodingException {
        GetRemoteClientConfigBody body = new GetRemoteClientConfigBody();
        RemoteClientConfig allClientConfigs = new RemoteClientConfig();
        Properties properties = MixAll.object2Properties(allClientConfigs);

        Enumeration keys = properties.keys();
        List<String> clientConfigKeys = new ArrayList<String>();
        while (keys.hasMoreElements()) {
            String nowKey = (String) keys.nextElement();
            if (!"log".equals(nowKey)) {
                clientConfigKeys.add(nowKey);
            }
        }

        body.setKeys(clientConfigKeys);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CLIENT_CONFIG, null);
        request.setBody(body.encode());
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

        assert response != null;
        Properties returnProperty;

        if (ResponseCode.SUCCESS == response.getCode()) {
            assert response.getBody().hasArray();
            returnProperty = MixAll.byteBuffer2Properties(response.getBody());
        } else {
            throw new MQClientException(response.getCode(), response.getRemark());
        }
        return returnProperty;
    }

    public QueryConsumeQueueResponseBody queryConsumeQueue(final String brokerAddr, final String topic,
                                                           final int queueId,
                                                           final long index, final int count, final String consumerGroup,
                                                           final long timeoutMillis) throws InterruptedException,
            RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {

        QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setIndex(index);
        requestHeader.setCount(count);
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;

        if (ResponseCode.SUCCESS == response.getCode()) {
            return JsonUtils.decode(response.getBody(), QueryConsumeQueueResponseBody.class);
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void checkClientInBroker(final String brokerAddr, final String consumerGroup,
                                    final String clientId, final SubscriptionData subscriptionData,
                                    final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

        CheckClientRequestBody requestBody = new CheckClientRequestBody();
        requestBody.setClientId(clientId);
        requestBody.setGroup(consumerGroup);
        requestBody.setSubscriptionData(subscriptionData);

        request.setBody(requestBody.encode());
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;

        if (ResponseCode.SUCCESS != response.getCode()) {
            throw new MQClientException(response.getCode(), response.getRemark());
        }
    }

    public void statisticsMessagesAsync(final String addr, final StatisticsMessagesRequestHeader requestHeader,
                                        final long timeoutMillis,
                                        final StatisticsMessagesCallback callback) throws RemotingException, InterruptedException {
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.STATISTICS_MESSAGES_V2, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new BaseInvokeCallback(MQClientAPIImpl.this) {
            @Override
            public void onComplete(ResponseFuture responseFuture) {
                RemotingCommand response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        StatisticsMessagesResult result = processStatisticsMessagesResponse(response);
                        assert request != null;
                        callback.onSuccess(result);
                    } catch (Exception e) {
                        callback.onException(e);
                    }
                } else {
                    if (!responseFuture.isSendRequestOK()) {
                        callback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                    } else if (responseFuture.isTimeout()) {
                        callback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                                responseFuture.getCause()));
                    } else {
                        callback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                    }
                }
            }
        });
    }

    public void unregisterBroker(
            final String namesrvAddr,
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        UnRegisterBrokerRequestHeader requestHeader = new UnRegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);
        requestHeader.setBrokerId(brokerId);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setClusterName(clusterName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_BROKER, requestHeader);
        RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.getLabel(), this.getClientId());
        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    private StatisticsMessagesResult processStatisticsMessagesResponse(final RemotingCommand response)
            throws MQBrokerException, RemotingCommandException {
        StatisticsMessagesResult result = null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                if (response.getBody() != null) {
                    return JsonUtils.decode(response.getBody(), StatisticsMessagesResult.class);
                }
                break;
            default:
                throw new MQBrokerException(response.getCode(), response.getRemark());
        }
        return result;
    }

    public String getClientId() {
        return clientConfig.buildMQClientId();
    }

    public void setConsumerStatsManager(ConsumerStatsManager consumerStatsManager) {
        this.consumerStatsManager = consumerStatsManager;
    }

    private void incPopRT(final String addr, final PopMessageRequestHeader requestHeader,
                          ResponseFuture responseFuture) {
        if (this.clientConfig.isRecordApiStats() && this.consumerStatsManager != null && responseFuture != null) {
            long popRT = System.currentTimeMillis() - responseFuture.getBeginTimestamp();
            this.consumerStatsManager.incPopRT(addr, "", requestHeader.getTopic(), popRT);
        }
    }

    private void incPopQPS(final String addr, final PopMessageRequestHeader requestHeader) {
        if (this.clientConfig.isRecordApiStats() && this.consumerStatsManager != null) {
            this.consumerStatsManager.incPopQPS(addr, "", requestHeader.getTopic());
        }
    }

    private void incPeekRT(final String addr, final PeekMessageRequestHeader requestHeader,
                           ResponseFuture responseFuture) {
        if (this.clientConfig.isRecordApiStats() && this.consumerStatsManager != null && responseFuture != null) {
            long peekRT = System.currentTimeMillis() - responseFuture.getBeginTimestamp();
            this.consumerStatsManager.incPeekRT(addr, "", requestHeader.getTopic(), peekRT);
        }
    }

    private void incPeekQPS(final String addr, final PeekMessageRequestHeader requestHeader) {
        if (this.clientConfig.isRecordApiStats() && this.consumerStatsManager != null) {
            this.consumerStatsManager.incPeekQPS(addr, "", requestHeader.getTopic());
        }
    }

    private void incAckRT(final String addr, final AckMessageRequestHeader requestHeader,
                          ResponseFuture responseFuture) {
        if (this.clientConfig.isRecordApiStats() && this.consumerStatsManager != null && responseFuture != null) {
            long ackRT = System.currentTimeMillis() - responseFuture.getBeginTimestamp();
            this.consumerStatsManager.incAckRT(addr, "", requestHeader.getTopic(), ackRT);
        }
    }

    private void incAckQPS(final String addr, final AckMessageRequestHeader requestHeader) {
        if (this.clientConfig.isRecordApiStats() && this.consumerStatsManager != null) {
            this.consumerStatsManager.incAckQPS(addr, "", requestHeader.getTopic());
        }
    }

    private void incChangeRT(final String addr, final ChangeInvisibleTimeRequestHeader requestHeader,
                             ResponseFuture responseFuture) {
        if (this.clientConfig.isRecordApiStats() && this.consumerStatsManager != null && responseFuture != null) {
            long changeRT = System.currentTimeMillis() - responseFuture.getBeginTimestamp();
            this.consumerStatsManager.incChangeRT(addr, "", requestHeader.getTopic(), changeRT);
        }
    }

    private void incChangeQPS(final String addr, final ChangeInvisibleTimeRequestHeader requestHeader) {
        if (this.clientConfig.isRecordApiStats() && this.consumerStatsManager != null) {
            this.consumerStatsManager.incChangeQPS(addr, "", requestHeader.getTopic());
        }
    }
}