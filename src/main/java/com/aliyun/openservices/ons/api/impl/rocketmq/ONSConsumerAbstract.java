package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceDispatcherType;
import com.alibaba.ons.open.trace.core.dispatch.NameServerAddressSetter;
import com.alibaba.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyHeuristic;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.MessageSelector;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.PropertyValueConst;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.impl.tracehook.OnsConsumeMessageHookImpl;
import com.aliyun.openservices.ons.api.impl.util.ClientLoggerUtil;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.spi.ConsumerInterceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

import com.aliyun.openservices.ons.api.spi.Interceptor;
import org.apache.commons.lang3.StringUtils;

public class ONSConsumerAbstract extends ONSClientAbstract {
    final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    protected final DefaultMQPushConsumer defaultMQPushConsumer;
    private final static int MAX_CACHED_MESSAGE_SIZE_IN_MIB = 2048;
    private final static int MIN_CACHED_MESSAGE_SIZE_IN_MIB = 16;
    private final static int MAX_CACHED_MESSAGE_AMOUNT = 50000;
    private final static int MIN_CACHED_MESSAGE_AMOUNT = 100;
    /** 默认值限制为512MiB */
    private int maxCachedMessageSizeInMiB = 512;
    /** 默认值限制为5000条 */
    private int maxCachedMessageAmount = 5000;

    protected final List<Interceptor<Admin>> interceptors = new ArrayList<Interceptor<Admin>>();

    protected AllocateMessageQueueStrategy getAllocateMessageQueueStrategy(final Properties properties) {
        String allocateMessageQueueStrategyName =
                StringUtils.trimToEmpty(properties.getProperty(PropertyKeyConst.ALLOCATE_MESSAGE_QUEUE_STRATEGY_NAME));

        if (PropertyValueConst.ALLOCATE_MESSAGE_QUEUE_HEURISTIC.equalsIgnoreCase(allocateMessageQueueStrategyName)) {
            return new AllocateMessageQueueAveragelyHeuristic();
        }

        if (PropertyValueConst.ALLOCATE_MESSAGE_QUEUE_CIRCLE.equalsIgnoreCase(allocateMessageQueueStrategyName)) {
            return new AllocateMessageQueueAveragelyByCircle();
        }

        String allocateMessageQueueStrategy = StringUtils.trimToEmpty(properties.getProperty(
                PropertyKeyConst.ALLOCATE_MESSAGE_QUEUE_STRATEGY));
        if (PropertyKeyConst.ALLOCATE_MESSAGE_QUEUE_STRATEGY.equalsIgnoreCase(allocateMessageQueueStrategy)) {
            return  new AllocateMessageQueueAveragelyByCircle();
        }
        return new AllocateMessageQueueAveragely();
    }

    public ONSConsumerAbstract(final Properties properties) {
        super(properties);

        String consumerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.ConsumerId));
        if (StringUtils.isEmpty(consumerGroup)) {
            throw new ONSClientException("ConsumerId property is null");
        }

        AllocateMessageQueueStrategy strategy = getAllocateMessageQueueStrategy(properties);
        this.defaultMQPushConsumer = new DefaultMQPushConsumer(
                this.getNamespace(), consumerGroup, new OnsClientRPCHook(provider), strategy);

        String maxReconsumeTimes = properties.getProperty(PropertyKeyConst.MaxReconsumeTimes);
        if (!UtilAll.isBlank(maxReconsumeTimes)) {
            try {
                this.defaultMQPushConsumer.setMaxReconsumeTimes(Integer.parseInt(maxReconsumeTimes));
            } catch (NumberFormatException ignored) {
            }
        }

        String maxBatchMessageCount = properties.getProperty(PropertyKeyConst.MAX_BATCH_MESSAGE_COUNT);
        if (!UtilAll.isBlank(maxBatchMessageCount)) {
            this.defaultMQPushConsumer.setPullBatchSize(Integer.valueOf(maxBatchMessageCount));
        }

        String consumeTimeout = properties.getProperty(PropertyKeyConst.ConsumeTimeout);
        if (!UtilAll.isBlank(consumeTimeout)) {
            try {
                this.defaultMQPushConsumer.setConsumeTimeout(Integer.parseInt(consumeTimeout));
            } catch (NumberFormatException ignored) {
            }
        }

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        this.defaultMQPushConsumer.setVipChannelEnabled(isVipChannelEnabled);

        String instanceName = StringUtils.defaultIfEmpty(properties.getProperty(PropertyKeyConst.InstanceName), this.buildIntanceName());
        this.defaultMQPushConsumer.setInstanceName(instanceName);
        this.defaultMQPushConsumer.setNamesrvAddr(this.getNameServerAddr());

        String consumeThreadNums = properties.getProperty(PropertyKeyConst.ConsumeThreadNums);
        if (!UtilAll.isBlank(consumeThreadNums)) {
            this.defaultMQPushConsumer.setConsumeThreadMin(Integer.valueOf(consumeThreadNums));
            this.defaultMQPushConsumer.setConsumeThreadMax(Integer.valueOf(consumeThreadNums));
        }

        String configuredCachedMessageAmount = properties.getProperty(PropertyKeyConst.MaxCachedMessageAmount, String.valueOf(maxCachedMessageAmount));
        if (!UtilAll.isBlank(configuredCachedMessageAmount)) {
            maxCachedMessageAmount = Math.min(MAX_CACHED_MESSAGE_AMOUNT, Integer.valueOf(configuredCachedMessageAmount));
            maxCachedMessageAmount = Math.max(MIN_CACHED_MESSAGE_AMOUNT, maxCachedMessageAmount);
            this.defaultMQPushConsumer.setPullThresholdForTopic(maxCachedMessageAmount);

        }

        String configuredCachedMessageSizeInMiB = properties.getProperty(PropertyKeyConst.MaxCachedMessageSizeInMiB, String.valueOf(maxCachedMessageSizeInMiB));
        if (!UtilAll.isBlank(configuredCachedMessageSizeInMiB)) {
            maxCachedMessageSizeInMiB = Math.min(MAX_CACHED_MESSAGE_SIZE_IN_MIB, Integer.valueOf(configuredCachedMessageSizeInMiB));
            maxCachedMessageSizeInMiB = Math.max(MIN_CACHED_MESSAGE_SIZE_IN_MIB, maxCachedMessageSizeInMiB);
            this.defaultMQPushConsumer.setPullThresholdSizeForTopic(maxCachedMessageSizeInMiB);
        }

        // 为Consumer增加消息轨迹回发模块
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
                tempProperties.put(OnsTraceConstants.TraceDispatcherType, OnsTraceDispatcherType.CONSUMER.name());
                String selectQueueEnableStr = properties.getProperty(PropertyKeyConst.MsgTraceSelectQueueEnable, "true");
                tempProperties.put(OnsTraceConstants.MsgTraceSelectQueueEnable, selectQueueEnableStr);
                AsyncArrayDispatcher dispatcher = new AsyncArrayDispatcher(tempProperties, provider, new NameServerAddressSetter() {
                    @Override
                    public String getNewNameServerAddress() {
                        return getNameServerAddr();
                    }
                });
                dispatcher.setHostConsumer(defaultMQPushConsumer.getDefaultMQPushConsumerImpl());
                traceDispatcher = dispatcher;
                this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(
                        new OnsConsumeMessageHookImpl(traceDispatcher));
            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data", e);
            }
        }
        List<Interceptor<Admin>> userInterceptors = new ArrayList<Interceptor<Admin>>();
        ServiceLoader<ConsumerInterceptor> serviceLoader = ServiceLoader.load(ConsumerInterceptor.class);
        for (ConsumerInterceptor consumerInterceptor : serviceLoader) {
            userInterceptors.add(consumerInterceptor);
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
        this.defaultMQPushConsumer.setNamesrvAddr(newAddrs);
        this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    protected void subscribe(String topic, String subExpression) {
        try {
            this.defaultMQPushConsumer.subscribe(topic, subExpression);
        } catch (MQClientException e) {
            throw new ONSClientException("defaultMQPushConsumer subscribe exception", e);
        }
    }

    protected void subscribe(final String topic, final MessageSelector selector) {
        String subExpression = "*";
        String type = com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType.TAG;
        if (selector != null) {
            if (selector.getType() == null) {
                throw new ONSClientException("Expression type is null!");
            }
            subExpression = selector.getSubExpression();
            type = selector.getType().name();
        }

        com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector messageSelector;
        if (com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType.SQL92.equals(type)) {
            messageSelector = com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector.bySql(subExpression);
        } else if (com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType.TAG.equals(type)) {
            messageSelector = com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector.byTag(subExpression);
        } else {
            throw new ONSClientException(String.format("Expression type %s is unknown!", type));
        }

        try {
            this.defaultMQPushConsumer.subscribe(topic, messageSelector);
        } catch (MQClientException e) {
            throw new ONSClientException("Consumer subscribe exception", e);
        }
    }

    protected void unsubscribe(String topic) {
        this.defaultMQPushConsumer.unsubscribe(topic);
    }

    @Override
    public void start() {
        try {
            if (this.started.compareAndSet(false, true)) {
                this.defaultMQPushConsumer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            this.defaultMQPushConsumer.shutdown();
        }
        super.shutdown();
    }

    protected ConsumeConcurrentlyStatus action2Status(Action action) {
        switch (action) {
            case CommitMessage:
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            case ReconsumeLater:
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            default:
                return null;
        }
    }

    protected ConsumeOrderlyStatus orderAction2Status(OrderAction action) {
        switch (action) {
            case Success:
                return ConsumeOrderlyStatus.SUCCESS;
            case Suspend:
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            default:
                return null;
        }
    }
}
