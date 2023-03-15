package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultLitePullConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.ons.api.Constants;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.PropertyValueConst;
import com.aliyun.openservices.ons.api.PullConsumer;
import com.aliyun.openservices.ons.api.TopicPartition;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.impl.util.ClientLoggerUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PullConsumerImpl extends ONSClientAbstract implements PullConsumer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    private final static int MAX_CACHED_MESSAGE_SIZE_IN_MIB = 1024;
    private final static int MIN_CACHED_MESSAGE_SIZE_IN_MIB = 16;
    private final static int MAX_CACHED_MESSAGE_AMOUNT = 50000;
    private final static int MIN_CACHED_MESSAGE_AMOUNT = 100;

    private DefaultLitePullConsumer litePullConsumer;

    private int maxCachedMessageSizeInMiB = 512;

    private int maxCachedMessageAmount = 5000;

    private long minAutoCommitIntervalMillis = 1 * 1000;

    public PullConsumerImpl(Properties properties) {
        super(properties);
        String consumerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.GROUP_ID));
        if (StringUtils.isEmpty(consumerGroup)) {
            throw new ONSClientException("Unable to get GROUP_ID property");
        }

        this.litePullConsumer =
            new DefaultLitePullConsumer(this.getNamespace(), consumerGroup, new OnsClientRPCHook(provider));

        String messageModel = properties.getProperty(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        this.litePullConsumer.setMessageModel(MessageModel.valueOf(messageModel));

        String maxBatchMessageCount = properties.getProperty(PropertyKeyConst.MAX_BATCH_MESSAGE_COUNT);
        if (!UtilAll.isBlank(maxBatchMessageCount)) {
            this.litePullConsumer.setPullBatchSize(Integer.valueOf(StringUtils.trim(maxBatchMessageCount)));
        }

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        this.litePullConsumer.setVipChannelEnabled(isVipChannelEnabled);

        String instanceName = StringUtils.defaultIfEmpty(properties.getProperty(PropertyKeyConst.InstanceName), this.buildIntanceName());
        this.litePullConsumer.setInstanceName(instanceName);
        this.litePullConsumer.setNamesrvAddr(this.getNameServerAddr());

        String consumeThreadNums = properties.getProperty(PropertyKeyConst.ConsumeThreadNums);
        if (!UtilAll.isBlank(consumeThreadNums)) {
            this.litePullConsumer.setPullThreadNums(Integer.valueOf(StringUtils.trim(consumeThreadNums)));
        }

        String configuredCachedMessageAmount = properties.getProperty(PropertyKeyConst.MaxCachedMessageAmount, String.valueOf(maxCachedMessageAmount));
        if (!UtilAll.isBlank(configuredCachedMessageAmount)) {
            maxCachedMessageAmount = Math.min(MAX_CACHED_MESSAGE_AMOUNT, Integer.valueOf(StringUtils.trim(configuredCachedMessageAmount)));
            maxCachedMessageAmount = Math.max(MIN_CACHED_MESSAGE_AMOUNT, maxCachedMessageAmount);
            this.litePullConsumer.setPullThresholdForAll(maxCachedMessageAmount);
        }

        String configuredCachedMessageSizeInMiB = properties.getProperty(PropertyKeyConst.MaxCachedMessageSizeInMiB, String.valueOf(maxCachedMessageSizeInMiB));
        if (!UtilAll.isBlank(configuredCachedMessageSizeInMiB)) {
            maxCachedMessageSizeInMiB = Math.min(MAX_CACHED_MESSAGE_SIZE_IN_MIB, Integer.valueOf(StringUtils.trim(configuredCachedMessageSizeInMiB)));
            maxCachedMessageSizeInMiB = Math.max(MIN_CACHED_MESSAGE_SIZE_IN_MIB, maxCachedMessageSizeInMiB);
            this.litePullConsumer.setPullThresholdSizeForQueue(maxCachedMessageSizeInMiB);
        }

        String autoCommit = properties.getProperty(PropertyKeyConst.AUTO_COMMIT);
        if (!UtilAll.isBlank(autoCommit)) {
            this.litePullConsumer.setAutoCommit(Boolean.valueOf(autoCommit));
        }

        String configuredAutoCommitIntervalMillis = properties.getProperty(PropertyKeyConst.AUTO_COMMIT_INTERVAL_MILLIS);
        if (!StringUtils.isBlank(configuredAutoCommitIntervalMillis)) {
            long autoCommitIntervalMillis = Math.max(minAutoCommitIntervalMillis, Long.valueOf(StringUtils.trim(configuredAutoCommitIntervalMillis)));
            this.litePullConsumer.setAutoCommitIntervalMillis(autoCommitIntervalMillis);
        }

        String configuredPollTimeoutMillis = properties.getProperty(PropertyKeyConst.POLL_TIMEOUT_MILLIS);
        if (!StringUtils.isBlank(configuredPollTimeoutMillis)) {
            this.litePullConsumer.setPollTimeoutMillis(Long.valueOf(StringUtils.trim(configuredPollTimeoutMillis)));
        }

    }

    @Override protected void updateNameServerAddr(String nameServerAddresses) {
        this.litePullConsumer.updateNameServerAddress(nameServerAddresses);
    }

    private Set<TopicPartition> convertToTopicPartitions(Collection<MessageQueue> messageQueues) {
        Set<TopicPartition> topicPartitions = new HashSet<TopicPartition>();
        for (MessageQueue messageQueue : messageQueues) {
            TopicPartition topicPartition = convertToTopicPartition(messageQueue);
            topicPartitions.add(topicPartition);
        }
        return topicPartitions;
    }

    private Set<MessageQueue> convertToMessageQueues(Collection<TopicPartition> topicPartitions) {
        Set<MessageQueue> messageQueues = new HashSet<MessageQueue>();
        for (TopicPartition topicPartition : topicPartitions) {
            messageQueues.add(convertToMessageQueue(topicPartition));
        }
        return messageQueues;
    }

    private TopicPartition convertToTopicPartition(MessageQueue messageQueue) {
        String topic = messageQueue.getTopic();
        String partition = messageQueue.getBrokerName() + Constants.TOPIC_PARTITION_SEPARATOR + messageQueue.getQueueId();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        return topicPartition;
    }

    private MessageQueue convertToMessageQueue(TopicPartition topicPartition) {
        String topic = topicPartition.getTopic();
        String[] tmp = topicPartition.getPartition().split(Constants.TOPIC_PARTITION_SEPARATOR);
        if (tmp.length != 2) {
            LOGGER.warn("Failed to get message queue from TopicPartition: {}", topicPartition);
            throw new ONSClientException("Failed to get message queue");
        }
        String brokerName = tmp[0];
        int queueId = Integer.valueOf(tmp[1]);
        return new MessageQueue(topic, brokerName, queueId);
    }

    @Override public Set<TopicPartition> topicPartitions(String topic) {
        try {
            Collection<MessageQueue> messageQueues = litePullConsumer.fetchMessageQueues(topic);
            Set<TopicPartition> topicPartitions = new HashSet<TopicPartition>();
            for (MessageQueue messageQueue : messageQueues) {
                topicPartitions.add(convertToTopicPartition(messageQueue));
            }
            return topicPartitions;
        } catch (MQClientException ex) {
            LOGGER.warn("Failed to fetch topic partitions", ex);
            throw new ONSClientException("Failed to fetch topic partitions", ex);
        }
    }

    @Override public void assign(Collection<TopicPartition> topicPartitions) {
        Set<MessageQueue> messageQueues = new HashSet<MessageQueue>();
        for (TopicPartition topicPartition : topicPartitions) {
            messageQueues.add(convertToMessageQueue(topicPartition));
        }
        this.litePullConsumer.assign(messageQueues);
    }

    @Override
    public void registerTopicPartitionChangedListener(String topic, final TopicPartitionChangeListener callback) {
        TopicMessageQueueChangeListener listener = new TopicMessageQueueChangeListener() {
            @Override public void onChanged(String topic, Set<MessageQueue> messageQueues) {
                callback.onChanged(convertToTopicPartitions(messageQueues));
            }
        };
        try {
            this.litePullConsumer.registerTopicMessageQueueChangeListener(topic, listener);

        } catch (MQClientException ex) {
            LOGGER.warn("Register listener error", ex);
            throw new ONSClientException("Failed to register topic partition listener");
        }
    }

    @Override public List<Message> poll(long timeout) {
        List<MessageExt> rmqMsgList = litePullConsumer.poll(timeout);
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
        return msgList;
    }

    @Override public void seek(TopicPartition topicPartition, long offset) {
        MessageQueue messageQueue = convertToMessageQueue(topicPartition);
        try {
            litePullConsumer.seek(messageQueue, offset);
        } catch (MQClientException ex) {
            LOGGER.warn("Topic partition: {} seek to: {} error", topicPartition, offset, ex);
            throw new ONSClientException("Seek offset failed");
        }
    }

    @Override public void seekToBeginning(TopicPartition topicPartition) {
        try {
            this.litePullConsumer.seekToBegin(convertToMessageQueue(topicPartition));
        } catch (MQClientException ex) {
            LOGGER.warn("Topic partition: {} seek to beginning error", topicPartition, ex);
            throw new ONSClientException("Seek offset to beginning failed");
        }
    }

    @Override public void seekToEnd(TopicPartition topicPartition) {
        try {
            this.litePullConsumer.seekToEnd(convertToMessageQueue(topicPartition));
        } catch (MQClientException ex) {
            LOGGER.warn("Topic partition: {} seek to end error", topicPartition, ex);
            throw new ONSClientException("Seek offset to end failed");
        }

    }

    @Override public void pause(Collection<TopicPartition> topicPartitions) {
        this.litePullConsumer.pause(convertToMessageQueues(topicPartitions));
    }

    @Override public void resume(Collection<TopicPartition> topicPartitions) {
        this.litePullConsumer.resume(convertToMessageQueues(topicPartitions));
    }

    @Override public Long offsetForTimestamp(TopicPartition topicPartition, Long timestamp) {
        try {
            return litePullConsumer.offsetForTimestamp(convertToMessageQueue(topicPartition), timestamp);
        } catch (MQClientException ex) {
            LOGGER.warn("Get offset for topic partition:{} with timestamp:{} error", topicPartition, timestamp, ex);
            throw new ONSClientException("Failed to get offset");
        }
    }

    @Override public Long committed(TopicPartition topicPartition) {
        try {
            return litePullConsumer.committed(convertToMessageQueue(topicPartition));
        } catch (MQClientException ex) {
            LOGGER.warn("Get committed offset for topic partition: {} error", topicPartition, ex);
            throw new ONSClientException("Failed to get committed offset");
        }
    }

    @Override public void commitSync() {
        litePullConsumer.commitSync();
    }

    @Override public void start() {
        try {
            if (this.started.compareAndSet(false, true)) {
                this.litePullConsumer.start();
                super.start();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to start pull consumer", e);
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            this.litePullConsumer.shutdown();
        }
        super.shutdown();
    }
}
