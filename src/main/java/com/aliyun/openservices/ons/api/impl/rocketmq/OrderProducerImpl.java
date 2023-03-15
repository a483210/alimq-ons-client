package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceDispatcherType;
import com.alibaba.ons.open.trace.core.dispatch.NameServerAddressSetter;
import com.alibaba.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.impl.tracehook.OnsClientSendMessageHookImpl;
import com.aliyun.openservices.ons.api.impl.util.ClientLoggerUtil;
import com.aliyun.openservices.ons.api.order.OrderProducer;
import com.aliyun.openservices.ons.api.spi.DefaultInvocationContext;
import com.aliyun.openservices.ons.api.spi.Interceptor;
import com.aliyun.openservices.ons.api.spi.ProducerInterceptor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

public class OrderProducerImpl extends ONSClientAbstract implements OrderProducer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    private final DefaultMQProducer defaultMQProducer;
    private final List<Interceptor<Admin>> interceptors = new ArrayList<Interceptor<Admin>>();

    public OrderProducerImpl(final Properties properties) {
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

        String sendMsgTimeoutMillis = properties.getProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        this.defaultMQProducer.setSendMsgTimeout(Integer.parseInt(sendMsgTimeoutMillis));

        boolean addExtendUniqInfo = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.EXACTLYONCE_DELIVERY, "false"));
        this.defaultMQProducer.setAddExtendUniqInfo(addExtendUniqInfo);

        String instanceName = StringUtils.defaultIfEmpty(properties.getProperty(PropertyKeyConst.InstanceName), this.buildIntanceName());
        this.defaultMQProducer.setInstanceName(instanceName);
        this.defaultMQProducer.setNamesrvAddr(this.getNameServerAddr());
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
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data", e);
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
            if (started.compareAndSet(false, true)) {
                this.defaultMQProducer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(final Message message, final String shardingKey) {
        if (UtilAll.isBlank(shardingKey)) {
            throw new ONSClientException("\'shardingKey\' is blank.");
        }
        message.setShardingKey(shardingKey);
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        DefaultInvocationContext invocationContext = new DefaultInvocationContext();
        invocationContext.setNamespaceId(this.defaultMQProducer.getNamespace());
        invocationContext.setMessages(Collections.singletonList(message));
        List<Runnable> postHandleStack = new ArrayList<Runnable>();
        boolean proceed = preHandle(interceptors, invocationContext, postHandleStack);
        final com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
        try {
            if (proceed) {
                com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult sendResultRMQ =
                    this.defaultMQProducer.send(msgRMQ, new com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msg,
                            Object shardingKey) {
                            int select = Math.abs(shardingKey.hashCode());
                            if (select < 0) {
                                select = 0;
                            }
                            return mqs.get(select % mqs.size());
                        }
                    }, shardingKey);
                message.setMsgID(sendResultRMQ.getMsgId());
                SendResult sendResult = new SendResult();
                sendResult.setTopic(message.getTopic());
                sendResult.setMessageId(sendResultRMQ.getMsgId());
                invocationContext.setSendResult(sendResult);
                return sendResult;
            }
        } catch (Exception e) {
            ONSClientException onsClientException = new ONSClientException("defaultMQProducer send order exception", e);
            invocationContext.setException(onsClientException);
            throw onsClientException;
        } finally {
            executePostHandle(postHandleStack);
        }
        throw new ONSClientException("ProducerInterceptor aborts sending");
    }
}
