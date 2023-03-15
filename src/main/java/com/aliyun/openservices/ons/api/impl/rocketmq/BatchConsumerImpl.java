package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.ons.api.*;
import com.aliyun.openservices.ons.api.batch.BatchConsumer;
import com.aliyun.openservices.ons.api.batch.BatchMessageListener;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.spi.DefaultInvocationContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Generated;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Generated("ons-client")
public class BatchConsumerImpl extends ONSConsumerAbstract implements BatchConsumer {
    private final static int MAX_BATCH_SIZE = 1024;
    private final static int MIN_BATCH_SIZE = 1;
    private final ConcurrentHashMap<String, BatchMessageListener> subscribeTable = new ConcurrentHashMap<String, BatchMessageListener>();

    public BatchConsumerImpl(final Properties properties) {
        super(properties);

        boolean postSubscriptionWhenPull = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.PostSubscriptionWhenPull, "false"));
        this.defaultMQPushConsumer.setPostSubscriptionWhenPull(postSubscriptionWhenPull);

        String messageModel = properties.getProperty(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        this.defaultMQPushConsumer.setMessageModel(MessageModel.valueOf(messageModel));

        String consumeBatchSize = properties.getProperty(PropertyKeyConst.ConsumeMessageBatchMaxSize);
        if (!UtilAll.isBlank(consumeBatchSize)) {
            int batchSize = Math.min(MAX_BATCH_SIZE, Integer.valueOf(consumeBatchSize));
            batchSize = Math.max(MIN_BATCH_SIZE, batchSize);
            this.defaultMQPushConsumer.setConsumeMessageBatchMaxSize(batchSize);
        }

        String timedBatchConsumeAwaitDuration = properties.getProperty(PropertyKeyConst.BatchConsumeMaxAwaitDurationInSeconds);
        if (!UtilAll.isBlank(timedBatchConsumeAwaitDuration)) {
            try {
                long duration = Long.parseLong(timedBatchConsumeAwaitDuration);
                this.defaultMQPushConsumer.setMaxBatchConsumeWaitTime(duration, TimeUnit.SECONDS);
            } catch (NumberFormatException e) {
                LOGGER.error("Number format error", e);
            } catch (MQClientException e) {
                LOGGER.error("Invalid value for BatchConsumeMaxAwaitDurationInSeconds", e);
            }
        }
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new BatchMessageListenerImpl(this.subscribeTable));
        super.start();
    }

    @Override
    public void subscribe(String topic, String subExpression, BatchMessageListener listener) {
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
    public void unsubscribe(String topic) {
        if (null != topic) {
            this.subscribeTable.remove(topic);
            super.unsubscribe(topic);
        }
    }

    class BatchMessageListenerImpl implements MessageListenerConcurrently {

        private final ConcurrentMap<String, BatchMessageListener> subscribeTable;

        public BatchMessageListenerImpl(ConcurrentMap<String, BatchMessageListener> subscribeTable) {
            this.subscribeTable = subscribeTable;
        }

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList,
            ConsumeConcurrentlyContext contextRMQ) {
            List<Message> msgList = new ArrayList<Message>();
            for (MessageExt rmqMsg : rmqMsgList) {
                Message msg = ONSUtil.msgConvert(rmqMsg);
                Map<String, String> propertiesMap = rmqMsg.getProperties();
                msg.setMsgID(rmqMsg.getMsgId());
                if (propertiesMap != null && propertiesMap.get(Constants.TRANSACTION_ID) != null) {
                    msg.setMsgID(propertiesMap.get(Constants.TRANSACTION_ID));
                }
                msgList.add(msg);
            }

            BatchMessageListener listener = subscribeTable.get(msgList.get(0).getTopic());
            if (null == listener) {
                throw new ONSClientException("BatchMessageListener is null");
            }

            final ConsumeContext context = new ConsumeContext();
            DefaultInvocationContext invocationContext = new DefaultInvocationContext();
            invocationContext.setNamespaceId(defaultMQPushConsumer.getNamespace());
            invocationContext.setConsumerGroup(NamespaceUtil.withoutNamespace(defaultMQPushConsumer.getConsumerGroup()));
            invocationContext.setMessages(msgList);
            List<Runnable> postHandleStack = new ArrayList<Runnable>();
            boolean proceed = preHandle(interceptors, invocationContext, postHandleStack);
            for (int i = 0; i < msgList.size(); i++) {
                if (StringUtils.isNotBlank(ONSUnitUtils.getMSHAUnitRetry(msgList.get(i)))) {
                    MessageAccessor.putProperty(rmqMsgList.get(i), MessageConst.PROPERTY_TRANSIENT_MSHA_RETRY, ONSUnitUtils.getMSHAUnitRetry(msgList.get(i)));
                }
            }
            try {
                if (proceed) {
                    Action action = listener.consume(msgList, context);
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
                contextRMQ.setAckIndex(context.getAcknowledgeIndex());
                executePostHandle(postHandleStack);
            }
        }
    }
}
