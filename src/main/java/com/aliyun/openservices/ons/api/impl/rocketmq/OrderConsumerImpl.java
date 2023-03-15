package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageSelector;
import com.aliyun.openservices.ons.api.ONSUnitUtils;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.order.ConsumeOrderContext;
import com.aliyun.openservices.ons.api.order.MessageOrderListener;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
import com.aliyun.openservices.ons.api.spi.DefaultInvocationContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class OrderConsumerImpl extends ONSConsumerAbstract implements OrderConsumer {
    private final ConcurrentHashMap<String, MessageOrderListener> subscribeTable = new ConcurrentHashMap<String, MessageOrderListener>();

    public OrderConsumerImpl(final Properties properties) {
        super(properties);
        String suspendTimeMillis = properties.getProperty(PropertyKeyConst.SuspendTimeMillis);
        if (!UtilAll.isBlank(suspendTimeMillis)) {
            try {
                this.defaultMQPushConsumer.setSuspendCurrentQueueTimeMillis(Long.parseLong(suspendTimeMillis));
            } catch (NumberFormatException ignored) {
            }
        }

        boolean enableOrderlyConsumeAccelerator = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.ENABLE_ORDERLY_CONSUME_ACCELERATOR, "false"));
        this.defaultMQPushConsumer.setOrderlyConsumeAccelerator(enableOrderlyConsumeAccelerator);
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderlyImpl());
        super.start();
    }

    @Override
    public void subscribe(String topic, String subExpression, MessageOrderListener listener) {
        if (null == topic) {
            throw new ONSClientException("topic is null");
        }

        if (null == listener) {
            throw new ONSClientException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, subExpression);
    }

    @Override
    public void subscribe(final String topic, final MessageSelector selector, final MessageOrderListener listener) {
        if (null == topic) {
            throw new ONSClientException("topic is null");
        }

        if (null == listener) {
            throw new ONSClientException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, selector);
    }

    class MessageListenerOrderlyImpl implements MessageListenerOrderly {

        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> arg0, ConsumeOrderlyContext arg1) {
            MessageExt msgRMQ = arg0.get(0);
            Message msg = ONSUtil.msgConvert(msgRMQ);
            msg.setMsgID(msgRMQ.getMsgId());

            MessageOrderListener listener = OrderConsumerImpl.this.subscribeTable.get(msg.getTopic());
            if (null == listener) {
                throw new ONSClientException("MessageOrderListener is null");
            }
            DefaultInvocationContext invocationContext = new DefaultInvocationContext();
            invocationContext.setNamespaceId(defaultMQPushConsumer.getNamespace());
            invocationContext.setConsumerGroup(NamespaceUtil.withoutNamespace(defaultMQPushConsumer.getConsumerGroup()));
            invocationContext.setMessages(Collections.singletonList(msg));
            List<Runnable> postHandleStack = new ArrayList<Runnable>();
            boolean proceed = preHandle(interceptors, invocationContext, postHandleStack);
            if (StringUtils.isNotBlank(ONSUnitUtils.getMSHAUnitRetry(msg))) {
                MessageAccessor.putProperty(msgRMQ, MessageConst.PROPERTY_TRANSIENT_MSHA_RETRY, ONSUnitUtils.getMSHAUnitRetry(msg));
            }
            try {
                if (proceed) {
                    final ConsumeOrderContext context = new ConsumeOrderContext();
                    OrderAction action = listener.consume(msg, context);
                    invocationContext.setOrderAction(action);
                    if (action == null) {
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                    return orderAction2Status(action);
                }
                if (!invocationContext.getOrderAction().isPresent()) {
                    // Skip processing directly once interceptor returns false.
                    return ConsumeOrderlyStatus.SUCCESS;
                }
                return orderAction2Status(invocationContext.getOrderAction().get());
            } finally {
                executePostHandle(postHandleStack);
            }
        }
    }
}
