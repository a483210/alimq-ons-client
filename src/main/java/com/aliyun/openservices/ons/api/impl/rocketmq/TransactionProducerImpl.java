package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceDispatcherType;
import com.alibaba.ons.open.trace.core.dispatch.NameServerAddressSetter;
import com.alibaba.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.ons.api.Constants;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.impl.tracehook.OnsClientSendMessageHookImpl;
import com.aliyun.openservices.ons.api.impl.util.ClientLoggerUtil;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionExecuter;
import com.aliyun.openservices.ons.api.transaction.TransactionProducer;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public class TransactionProducerImpl extends ONSClientAbstract implements TransactionProducer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    TransactionMQProducer transactionMQProducer = null;
    private Properties properties;

    public TransactionProducerImpl(Properties properties, TransactionCheckListener transactionCheckListener) {
        super(properties);
        this.properties = properties;
        String producerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.ProducerId));
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = "__ONS_PRODUCER_DEFAULT_GROUP";
        }
        transactionMQProducer =
            new TransactionMQProducer(this.getNamespace(), producerGroup, new OnsClientRPCHook(provider));

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        transactionMQProducer.setVipChannelEnabled(isVipChannelEnabled);

        String instanceName = StringUtils.defaultIfEmpty(properties.getProperty(PropertyKeyConst.InstanceName), this.buildIntanceName());
        this.transactionMQProducer.setInstanceName(instanceName);

        boolean addExtendUniqInfo = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.EXACTLYONCE_DELIVERY, "false"));
        transactionMQProducer.setAddExtendUniqInfo(addExtendUniqInfo);

        transactionMQProducer.setTransactionCheckListener(transactionCheckListener);
        // 为Producer增加消息轨迹回发模块
        String msgTraceSwitch = properties.getProperty(PropertyKeyConst.MsgTraceSwitch);
        if (!UtilAll.isBlank(msgTraceSwitch) && (!Boolean.parseBoolean(msgTraceSwitch))) {
            LOGGER.info("MQ Client Disable the Trace Hook!");
        } else {
            try {
                Properties tempProperties = new Properties();
                tempProperties.put(OnsTraceConstants.AccessKey, sessionCredentials.getAccessKey());
                tempProperties.put(OnsTraceConstants.SecretKey, sessionCredentials.getSecretKey());
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
                dispatcher.setHostProducer(transactionMQProducer.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.transactionMQProducer.getDefaultMQProducerImpl().registerSendMessageHook(
                    new OnsClientSendMessageHookImpl(traceDispatcher));
            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data", e);
            }
        }
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            if (transactionMQProducer.getTransactionCheckListener() == null) {
                throw new IllegalArgumentException("TransactionCheckListener is null");
            }
            // TODO 完善寻址功能。要实现TransactionMQProducer.sendMessageInTransaction
            transactionMQProducer.setNamesrvAddr(this.nameServerAddr);
            try {
                transactionMQProducer.start();
                super.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.transactionMQProducer.setNamesrvAddr(newAddrs);
        this.transactionMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            transactionMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(final Message message, final LocalTransactionExecuter executer, Object arg) {
        this.checkONSProducerServiceState(this.transactionMQProducer.getDefaultMQProducerImpl());
        com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
        com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.TransactionSendResult sendResultRMQ = null;
        try {
            sendResultRMQ = transactionMQProducer.sendMessageInTransaction(msgRMQ,
                new com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.LocalTransactionExecuter() {
                    @Override
                    public LocalTransactionState executeLocalTransactionBranch(
                        com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msg,
                        Object arg) {
                        String msgId = msg.getProperty(Constants.UNIQ_KEY);
                        message.setMsgID(msgId);
                        TransactionStatus transactionStatus = executer.execute(message, arg);
                        if (TransactionStatus.CommitTransaction == transactionStatus) {
                            return LocalTransactionState.COMMIT_MESSAGE;
                        } else if (TransactionStatus.RollbackTransaction == transactionStatus) {
                            return LocalTransactionState.ROLLBACK_MESSAGE;
                        }
                        return LocalTransactionState.UNKNOW;
                    }
                }, arg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (sendResultRMQ.getLocalTransactionState() == LocalTransactionState.ROLLBACK_MESSAGE) {
            // 本地事务显式说明了有异常，此时给应用方返回一个异常。
            throw new RuntimeException("local transaction branch failed ,so transaction rollback");
        }
        SendResult sendResult = new SendResult();
        sendResult.setMessageId(sendResultRMQ.getMsgId());
        sendResult.setTopic(sendResultRMQ.getMessageQueue().getTopic());
        return sendResult;
    }

}
