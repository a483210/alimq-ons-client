package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.ons.api.Constants;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageAccessor;
import com.aliyun.openservices.ons.api.exception.ONSClientException;

public class ONSUtil {
    private static final Set<String> RESERVED_KEY_SET_RMQ = new HashSet<String>();

    private static final Set<String> RESERVED_KEY_SET_ONS = new HashSet<String>();

    static {

        /**
         * RMQ
         */
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_KEYS);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_TAGS);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_RETRY_TOPIC);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_REAL_TOPIC);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_REAL_QUEUE_ID);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_PRODUCER_GROUP);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_MIN_OFFSET);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_MAX_OFFSET);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_TRANSIENT_MSHA_RETRY);
        RESERVED_KEY_SET_RMQ.add(MessageConst.PROPERTY_MSHA_TRANSFORM);
        /**
         * ONS
         */
        RESERVED_KEY_SET_ONS.add(Message.SystemPropKey.TAG);
        RESERVED_KEY_SET_ONS.add(Message.SystemPropKey.KEY);
        RESERVED_KEY_SET_ONS.add(Message.SystemPropKey.MSGID);
        RESERVED_KEY_SET_ONS.add(Message.SystemPropKey.RECONSUMETIMES);
        RESERVED_KEY_SET_ONS.add(Message.SystemPropKey.STARTDELIVERTIME);
        RESERVED_KEY_SET_ONS.add(Message.SystemPropKey.BORNHOST);
        RESERVED_KEY_SET_ONS.add(Message.SystemPropKey.BORNTIMESTAMP);
        RESERVED_KEY_SET_ONS.add(Message.SystemPropKey.SHARDINGKEY);
    }

    public static com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgConvert(
        com.aliyun.openservices.ons.api.Message message) {
        com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ = new com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message();
        if (message == null) {
            throw new ONSClientException("\'message\' is null");
        }

        if (message.getTopic() != null) {
            msgRMQ.setTopic(message.getTopic());
        }
        if (message.getKey() != null) {
            msgRMQ.setKeys(message.getKey());
        }
        if (message.getTag() != null) {
            msgRMQ.setTags(message.getTag());
        }
        if (message.getStartDeliverTime() > 0) {
            msgRMQ.putUserProperty(Message.SystemPropKey.STARTDELIVERTIME, String.valueOf(message.getStartDeliverTime()));
        }
        if (message.getBody() != null) {
            msgRMQ.setBody(message.getBody());
        }

        if (message.getShardingKey() != null && !message.getShardingKey().isEmpty()) {
            msgRMQ.putUserProperty(Message.SystemPropKey.SHARDINGKEY, message.getShardingKey());
        }

        Properties systemProperties = MessageAccessor.getSystemProperties(message);
        if (systemProperties != null) {
            Iterator<Entry<Object, Object>> it = systemProperties.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Object, Object> next = it.next();
                if (!RESERVED_KEY_SET_ONS.contains(next.getKey().toString())) {
                    com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor.putProperty(msgRMQ, next.getKey().toString(),
                        next.getValue().toString());
                }
            }
        }

        Properties userProperties = message.getUserProperties();
        if (userProperties != null) {
            Iterator<Entry<Object, Object>> it = userProperties.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Object, Object> next = it.next();
                if (!RESERVED_KEY_SET_RMQ.contains(next.getKey().toString())) {
                    com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor.putProperty(msgRMQ, next.getKey().toString(),
                        next.getValue().toString());
                }
            }
        }

        return msgRMQ;
    }

    public static com.aliyun.openservices.ons.api.Message msgConvert(
        com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ) {
        com.aliyun.openservices.ons.api.Message message = new com.aliyun.openservices.ons.api.Message();
        if (msgRMQ.getTopic() != null) {
            message.setTopic(msgRMQ.getTopic());
        }
        if (msgRMQ.getKeys() != null) {
            message.setKey(msgRMQ.getKeys());
        }
        if (msgRMQ.getTags() != null) {
            message.setTag(msgRMQ.getTags());
        }
        if (msgRMQ.getBody() != null) {
            message.setBody(msgRMQ.getBody());
        }

        if (((MessageExt) msgRMQ).getBrokerName() != null) {
            String partition = ((MessageExt) msgRMQ).getBrokerName() + Constants.TOPIC_PARTITION_SEPARATOR + ((MessageExt) msgRMQ).getQueueId();
            MessageAccessor.putSystemProperties(message, Message.SystemPropKey.PARTITION, partition);
        }
        MessageAccessor.putSystemProperties(message, Message.SystemPropKey.CONSUMEOFFSET, String.valueOf(((MessageExt) msgRMQ).getQueueOffset()));

        message.setReconsumeTimes(((MessageExt) msgRMQ).getReconsumeTimes());
        message.setBornTimestamp(((MessageExt) msgRMQ).getBornTimestamp());
        message.setBornHost(String.valueOf(((MessageExt) msgRMQ).getBornHost()));

        Map<String, String> properties = msgRMQ.getProperties();
        if (properties != null) {
            Iterator<Entry<String, String>> it = properties.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, String> next = it.next();
                // System
                if (RESERVED_KEY_SET_RMQ.contains(next.getKey()) || RESERVED_KEY_SET_ONS.contains(next.getKey())) {
                    MessageAccessor.putSystemProperties(message, next.getKey(), next.getValue());
                }
                // User
                else {
                    message.putUserProperties(next.getKey(), next.getValue());
                }
            }
        }

        return message;
    }

    public static Properties extractProperties(final Properties properties) {
        Properties newPro = new Properties();
        Properties inner = null;
        try {
            Field field = Properties.class.getDeclaredField("defaults");
            field.setAccessible(true);
            inner = (Properties) field.get(properties);
        } catch (Exception ignore) {
        }

        if (inner != null) {
            for (final Entry<Object, Object> entry : inner.entrySet()) {
                newPro.setProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }

        for (final Entry<Object, Object> entry : properties.entrySet()) {
            newPro.setProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }

        return newPro;
    }
}
