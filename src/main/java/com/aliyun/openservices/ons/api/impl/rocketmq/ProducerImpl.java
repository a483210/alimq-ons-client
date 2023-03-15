package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceDispatcherType;
import com.alibaba.ons.open.trace.core.dispatch.NameServerAddressSetter;
import com.alibaba.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageClientIDSetter;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.OnExceptionContext;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendCallback;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.impl.tracehook.OnsClientSendMessageHookImpl;
import com.aliyun.openservices.ons.api.impl.util.ClientLoggerUtil;
import com.aliyun.openservices.ons.api.spi.DefaultInvocationContext;
import com.aliyun.openservices.ons.api.spi.Interceptor;
import com.aliyun.openservices.ons.api.spi.ProducerInterceptor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;

public class ProducerImpl extends ONSClientAbstract implements Producer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    private final DefaultMQProducer defaultMQProducer;
    private final List<Interceptor<Admin>> interceptors = new ArrayList<Interceptor<Admin>>();

    public ProducerImpl(final Properties properties) {
        super(properties);

        String producerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.ProducerId));
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = "__ONS_PRODUCER_DEFAULT_GROUP";
        }

        this.defaultMQProducer =
            new DefaultMQProducer(this.getNamespace(), producerGroup, new OnsClientRPCHook(provider));

        this.defaultMQProducer.setProducerGroup(producerGroup);

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        this.defaultMQProducer.setVipChannelEnabled(isVipChannelEnabled);

        if (properties.containsKey(PropertyKeyConst.SendMsgTimeoutMillis)) {
            this.defaultMQProducer.setSendMsgTimeout(Integer.valueOf(properties.get(PropertyKeyConst.SendMsgTimeoutMillis).toString()));
        } else {
            this.defaultMQProducer.setSendMsgTimeout(5000);
        }

        if (properties.containsKey(PropertyKeyConst.EXACTLYONCE_DELIVERY)) {
            this.defaultMQProducer.setAddExtendUniqInfo(Boolean.valueOf(properties.get(PropertyKeyConst.EXACTLYONCE_DELIVERY).toString()));
        }

        String instanceName = StringUtils.defaultIfEmpty(properties.getProperty(PropertyKeyConst.InstanceName), this.buildIntanceName());
        this.defaultMQProducer.setInstanceName(instanceName);
        this.defaultMQProducer.setNamesrvAddr(this.getNameServerAddr());
        // 消息最大大小4M
        this.defaultMQProducer.setMaxMessageSize(1024 * 1024 * 4);
        // 为Producer增加消息轨迹回发模块
        String msgTraceSwitch = properties.getProperty(PropertyKeyConst.MsgTraceSwitch);
        if (!UtilAll.isBlank(msgTraceSwitch) && (!Boolean.parseBoolean(msgTraceSwitch))) {
            LOGGER.info("MQ Client Disable the Trace Hook!");
        } else {
            try {
                Properties tempProperties = new Properties();
                tempProperties.put(OnsTraceConstants.MaxMsgSize, "128000");
                tempProperties.put(OnsTraceConstants.AsyncBufferSize, "2048");
                tempProperties.put(OnsTraceConstants.MaxBatchNum, "100");
                String traceInstanceName = UtilAll.getPid() + "_CLIENT_INNER_TRACE_PRODUCER";
                tempProperties.put(OnsTraceConstants.InstanceName, traceInstanceName);
                tempProperties.put(OnsTraceConstants.TraceDispatcherType, OnsTraceDispatcherType.PRODUCER.name());
                String selectQueueEnableStr = properties.getProperty(PropertyKeyConst.MsgTraceSelectQueueEnable, "true");
                tempProperties.put(OnsTraceConstants.MsgTraceSelectQueueEnable, selectQueueEnableStr);
                AsyncArrayDispatcher dispatcher = new AsyncArrayDispatcher(tempProperties, provider, new NameServerAddressSetter() {
                    @Override
                    public String getNewNameServerAddress() {
                        return getNameServerAddr();
                    }
                });
                dispatcher.setHostProducer(defaultMQProducer.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.defaultMQProducer.getDefaultMQProducerImpl().registerSendMessageHook(
                    new OnsClientSendMessageHookImpl(traceDispatcher));
            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data.", e);
            }
        }
        List<Interceptor<Admin>> userInterceptors = new ArrayList<Interceptor<Admin>>();
        ServiceLoader<ProducerInterceptor> serviceLoader = ServiceLoader.load(ProducerInterceptor.class);
        for (ProducerInterceptor producerInterceptor : serviceLoader) {
            userInterceptors.add(producerInterceptor);
        }
        registerInterceptor(userInterceptors);
    }

    // 可以作为AOP切点
    private void registerInterceptor(List<Interceptor<Admin>> userInterceptors) {
        if (userInterceptors == null || userInterceptors.isEmpty()) {
            return;
        }
        interceptors.addAll(userInterceptors);
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.defaultMQProducer.setNamesrvAddr(newAddrs);
        this.defaultMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    @Override
    public void start() {
        try {
            if (this.started.compareAndSet(false, true)) {
                this.defaultMQProducer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            this.defaultMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(Message message) {
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        DefaultInvocationContext invocationContext = new DefaultInvocationContext();
        invocationContext.setNamespaceId(this.defaultMQProducer.getNamespace());
        invocationContext.setMessages(Collections.singletonList(message));
        final List<Runnable> postHandleStack = new ArrayList<Runnable>();
        boolean proceed = preHandle(interceptors, invocationContext, postHandleStack);
        try {
            if (proceed) {
                com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
                try {
                    com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult sendResultRMQ = this.defaultMQProducer.send(msgRMQ);
                    message.setMsgID(sendResultRMQ.getMsgId());
                    SendResult sendResult = new SendResult();
                    sendResult.setTopic(sendResultRMQ.getMessageQueue().getTopic());
                    sendResult.setMessageId(sendResultRMQ.getMsgId());
                    invocationContext.setSendResult(sendResult);
                    return sendResult;
                } catch (Exception e) {
                    LOGGER.error(String.format("Send message Exception, %s", message), e);
                    ONSClientException onsClientException = checkProducerException(message.getTopic(), message.getMsgID(), e);
                    invocationContext.setException(onsClientException);
                    throw onsClientException;
                }
            }
        } finally {
            executePostHandle(postHandleStack);
        }
        throw new ONSClientException("ProducerInterceptor aborts sending");
    }

    @Override
    public void sendOneway(Message message) {
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        DefaultInvocationContext invocationContext = new DefaultInvocationContext();
        invocationContext.setNamespaceId(this.defaultMQProducer.getNamespace());
        invocationContext.setMessages(Collections.singletonList(message));
        List<Runnable> postHandleStack = new ArrayList<Runnable>();
        boolean proceed = preHandle(interceptors, invocationContext, postHandleStack);
        try {
            if (proceed) {
                com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
                try {
                    this.defaultMQProducer.sendOneway(msgRMQ);
                    message.setMsgID(MessageClientIDSetter.getUniqID(msgRMQ));
                } catch (Exception e) {
                    LOGGER.error(String.format("Send message oneway Exception, %s", message), e);
                    ONSClientException onsClientException = checkProducerException(message.getTopic(),
                        message.getMsgID(), e);
                    invocationContext.setException(onsClientException);
                    throw onsClientException;
                }
            }
        } finally {
            executePostHandle(postHandleStack);
        }
    }

    @Override
    public void sendAsync(Message message, SendCallback sendCallback) {
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        DefaultInvocationContext invocationContext = new DefaultInvocationContext();
        invocationContext.setNamespaceId(this.defaultMQProducer.getNamespace());
        invocationContext.setMessages(Collections.singletonList(message));
        List<Runnable> postHandleStack = new ArrayList<Runnable>();
        boolean proceed = preHandle(interceptors, invocationContext, postHandleStack);
        if (proceed) {
            com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
            try {
                this.defaultMQProducer.send(msgRMQ, sendCallbackConvert(message, sendCallback, postHandleStack, invocationContext));
                message.setMsgID(MessageClientIDSetter.getUniqID(msgRMQ));
            } catch (Exception e) {
                LOGGER.error(String.format("Send message async Exception, %s", message), e);
                ONSClientException onsClientException = checkProducerException(message.getTopic(), message.getMsgID(), e);
                invocationContext.setException(onsClientException);
                executePostHandle(postHandleStack);
                throw onsClientException;
            }
        }
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.defaultMQProducer.setCallbackExecutor(callbackExecutor);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    private com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendCallback sendCallbackConvert(final Message message,
        final com.aliyun.openservices.ons.api.SendCallback sendCallback, final List<Runnable> postHandleStack,
        final DefaultInvocationContext invocationContext) {
        return new com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendCallback() {
            @Override
            public void onSuccess(com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult sendResult) {
                try {
                    SendResult result = sendResultConvert(sendResult);
                    invocationContext.setMessages(Collections.singletonList(message));
                    invocationContext.setSendResult(result);
                    sendCallback.onSuccess(result);
                } finally {
                    executePostHandle(postHandleStack);
                }
            }

            @Override
            public void onException(Throwable e) {
                try {
                    String topic = message.getTopic();
                    String msgId = message.getMsgID();
                    ONSClientException onsEx = checkProducerException(topic, msgId, e);
                    OnExceptionContext context = new OnExceptionContext();
                    context.setTopic(topic);
                    context.setMessageId(msgId);
                    context.setException(onsEx);
                    invocationContext.setMessages(Collections.singletonList(message));
                    invocationContext.setException(onsEx);
                    sendCallback.onException(context);
                } finally {
                    executePostHandle(postHandleStack);
                }
            }
        };
    }

    private com.aliyun.openservices.ons.api.SendResult sendResultConvert(
        final com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult rmqSendResult) {
        com.aliyun.openservices.ons.api.SendResult sendResult = new com.aliyun.openservices.ons.api.SendResult();
        sendResult.setTopic(rmqSendResult.getMessageQueue().getTopic());
        sendResult.setMessageId(rmqSendResult.getMsgId());
        return sendResult;
    }

    private ONSClientException checkProducerException(String topic, String msgId, Throwable e) {
        if (e instanceof MQClientException) {
            //
            if (e.getCause() != null) {
                // 无法连接Broker
                if (e.getCause() instanceof RemotingConnectException) {
                    return new ONSClientException(
                        FAQ.errorMessage(String.format("Connect broker failed, Topic=%s, msgId=%s", topic, msgId), FAQ.CONNECT_BROKER_FAILED));
                }
                // 发送消息超时
                else if (e.getCause() instanceof RemotingTimeoutException) {
                    return new ONSClientException(FAQ.errorMessage(String.format("Send message to broker timeout, %dms, Topic=%s, msgId=%s",
                        this.defaultMQProducer.getSendMsgTimeout(), topic, msgId), FAQ.SEND_MSG_TO_BROKER_TIMEOUT));
                }
                // Broker返回异常
                else if (e.getCause() instanceof MQBrokerException) {
                    MQBrokerException excep = (MQBrokerException) e.getCause();
                    return new ONSClientException(FAQ.errorMessage(
                        String.format("Receive a broker exception, Topic=%s, msgId=%s, %s", topic, msgId, excep.getErrorMessage()),
                        FAQ.BROKER_RESPONSE_EXCEPTION));
                }
            }
            // 纯客户端异常
            else {
                MQClientException excep = (MQClientException) e;
                if (-1 == excep.getResponseCode()) {
                    return new ONSClientException(
                        FAQ.errorMessage(String.format("Topic does not exist, Topic=%s, msgId=%s", topic, msgId), FAQ.TOPIC_ROUTE_NOT_EXIST));
                } else if (ResponseCode.MESSAGE_ILLEGAL == excep.getResponseCode()) {
                    return new ONSClientException(
                        FAQ.errorMessage(String.format("ONS Client check message exception, Topic=%s, msgId=%s", topic, msgId),
                            FAQ.CLIENT_CHECK_MSG_EXCEPTION));
                }
            }
        }

        return new ONSClientException("defaultMQProducer send exception", e);
    }
}
