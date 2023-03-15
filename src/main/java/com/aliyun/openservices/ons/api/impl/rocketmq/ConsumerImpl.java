package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.ons.api.*;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.spi.DefaultInvocationContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Generated;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Generated("ons-client")
public class ConsumerImpl extends ONSConsumerAbstract implements Consumer {
    private final ConcurrentHashMap<String, MessageListener> subscribeTable = new ConcurrentHashMap<String, MessageListener>();

    public ConsumerImpl(final Properties properties) {
        super(properties);
        boolean postSubscriptionWhenPull = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.PostSubscriptionWhenPull, "false"));
        this.defaultMQPushConsumer.setPostSubscriptionWhenPull(postSubscriptionWhenPull);

        String messageModel = properties.getProperty(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        this.defaultMQPushConsumer.setMessageModel(MessageModel.valueOf(messageModel));
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new MessageListenerImpl());
        super.start();
    }


    @Override
    public void subscribe(String topic, String subExpression, MessageListener listener) {
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
    public void subscribe(final String topic, final MessageSelector selector, final MessageListener listener) {
        if (null == topic) {
            throw new ONSClientException("topic is null");
        }

        if (null == listener) {
            throw new ONSClientException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, selector);
    }


    @Override
    public void unsubscribe(String topic) {
        if (null != topic) {
            this.subscribeTable.remove(topic);
            super.unsubscribe(topic);
        }
    }


    class MessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgsRMQList,
            ConsumeConcurrentlyContext contextRMQ) {
            MessageExt msgRMQ = msgsRMQList.get(0);
            Message msg = ONSUtil.msgConvert(msgRMQ);
            Map<String, String> stringStringMap = msgRMQ.getProperties();
            msg.setMsgID(msgRMQ.getMsgId());
            MessageListener listener = ConsumerImpl.this.subscribeTable.get(msg.getTopic());
            if (null == listener) {
                throw new ONSClientException("MessageListener is null");
            }

            final ConsumeContext context = new ConsumeContext();
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
                    Action action = listener.consume(msg, context);
                    invocationContext.setAction(action);
                    if (action == null) {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                    return action2Status(action);
                }
                if (!invocationContext.getAction().isPresent()) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                return action2Status(invocationContext.getAction().get());
            } finally {
                executePostHandle(postHandleStack);
            }
        }
    }
}
