package com.alibaba.ons.open.trace.core.dispatch.impl;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceContext;
import com.alibaba.ons.open.trace.core.common.OnsTraceDataEncoder;
import com.alibaba.ons.open.trace.core.common.OnsTraceTransferBean;
import com.alibaba.ons.open.trace.core.dispatch.NameServerAddressSetter;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.common.ThreadLocalIndex;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendCallback;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RPCHook;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentialsProvider;
import com.aliyun.openservices.ons.api.impl.rocketmq.ClientRPCHook;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author MQDevelopers
 */
public class InnerTraceProducer {
    private final static InternalLogger CLIENT_LOG = ClientLogger.getLog();
    private static final AtomicInteger instanceNum = new AtomicInteger(0);
    private final int instanceId = instanceNum.getAndIncrement();
    private final int queueSize;
    private final int batchSize;
    private final int threadNum = Math.max(8, Runtime.getRuntime().availableProcessors());
    private final DefaultMQProducer traceProducer;
    private final ThreadPoolExecutor traceExecuter;
    private final ScheduledExecutorService scheduledExecutorService;
    private AtomicLong discardCount;
    private Thread worker;
    private ArrayBlockingQueue<OnsTraceContext> traceContextQueue;
    private ArrayBlockingQueue<Runnable> appenderQueue;
    private volatile Thread shutDownHook;
    private volatile boolean stopped = false;
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private NameServerAddressSetter nameserverAddressSetter;
    private String traceNamesrvAddr;
    private static Map<String, InnerTraceProducer> dispatcherTable = new ConcurrentHashMap<String, InnerTraceProducer>();
    private static AtomicBoolean isStarted = new AtomicBoolean(false);
    private static InnerTraceProducer innerTraceProducer;
    private final Boolean selectQueueEnable;

    public static InnerTraceProducer getTraceDispatcherProducer(Properties properties,
        SessionCredentialsProvider provider,
        NameServerAddressSetter nameserverAddressSetter) throws MQClientException {
        return getTraceDispatcherProducer(properties, nameserverAddressSetter, new ClientRPCHook(provider));
    }

    public static InnerTraceProducer getTraceDispatcherProducer(Properties properties,
        NameServerAddressSetter nameserverAddressSetter, RPCHook rpcHook) throws MQClientException {
        boolean innerProducerSingleton = Boolean.parseBoolean(properties.getProperty(OnsTraceConstants.TraceProducerSingleton, "true"));
        if (!innerProducerSingleton) {
            return new InnerTraceProducer(properties, nameserverAddressSetter, rpcHook);
        }
        if (innerTraceProducer == null) {
            innerTraceProducer = new InnerTraceProducer(properties, nameserverAddressSetter, rpcHook);
        }
        return innerTraceProducer;
    }

    public static void registerTraceDispatcher(String dispatcherId, InnerTraceProducer producer,
        boolean isSingleton) throws MQClientException {
        dispatcherTable.put(dispatcherId, producer);
        if (isSingleton) {
            if (producer != null && isStarted.compareAndSet(false, true)) {
                producer.start();
            }
        } else {
            if (producer != null) {
                producer.start();
            }
        }
    }

    public static void unregisterTraceDispatcher(String dispatcherId, InnerTraceProducer producer,
        boolean isSingleton) {
        dispatcherTable.remove(dispatcherId);
        if (isSingleton) {
            if (dispatcherTable.isEmpty() && producer != null && isStarted.get()) {
                producer.shutdown();
            }
        } else {
            producer.shutdown();
        }
    }

    public boolean append(final OnsTraceContext ctx) {
        boolean result = traceContextQueue.offer(ctx);
        if (!result) {
            CLIENT_LOG.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    public InnerTraceProducer(Properties properties,
        final NameServerAddressSetter nameserverAddressSetter, RPCHook rpcHook) throws MQClientException {
        int queueSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.AsyncBufferSize, "2048"));
        // queueSize 取大于或等于 value 的 2 的 n 次方数
        queueSize = 1 << (32 - Integer.numberOfLeadingZeros(queueSize - 1));
        this.queueSize = queueSize;
        batchSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.MaxBatchNum, "1"));
        this.discardCount = new AtomicLong(0L);
        traceContextQueue = new ArrayBlockingQueue<OnsTraceContext>(1024);
        appenderQueue = new ArrayBlockingQueue<Runnable>(this.queueSize);

        this.traceExecuter = new ThreadPoolExecutor(//
            threadNum, //
            threadNum, //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.appenderQueue, //
            new ThreadFactoryImpl("MQTraceSendThread_" + instanceId + "_"));
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "OnsTrace-UpdateNameServerThread" + instanceId);
                }
            });
        this.nameserverAddressSetter = nameserverAddressSetter;
        this.selectQueueEnable = Boolean.parseBoolean(properties.getProperty(OnsTraceConstants.MsgTraceSelectQueueEnable, "true"));
        traceProducer = createProducer(properties, nameserverAddressSetter, rpcHook);
    }

    private DefaultMQProducer createProducer(Properties properties,
        NameServerAddressSetter nameserverAddressSetter, RPCHook rpcHook) {
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setProducerGroup(properties.getProperty(OnsTraceConstants.AccessKey) + OnsTraceConstants.groupName);
        producer.setSendMsgTimeout(5000);
        producer.setInstanceName(properties.getProperty(OnsTraceConstants.InstanceName, String.valueOf(System.currentTimeMillis())));
        producer.setNamesrvAddr(nameserverAddressSetter.getNewNameServerAddress());
        producer.setVipChannelEnabled(false);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 消息最大大小128K
        int maxSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.MaxMsgSize, "128000"));
        producer.setMaxMessageSize(maxSize - 10 * 1000);
        return producer;
    }

    private DefaultMQProducer createProducer(Properties properties, SessionCredentialsProvider provider,
        NameServerAddressSetter nameserverAddressSetter) {
        String accessKey = properties.getProperty(OnsTraceConstants.AccessKey);
        DefaultMQProducer producer = new DefaultMQProducer(new ClientRPCHook(provider));
        producer.setProducerGroup(accessKey.replace('.', '-') + OnsTraceConstants.groupName);
        producer.setSendMsgTimeout(5000);
        producer.setInstanceName(properties.getProperty(OnsTraceConstants.InstanceName, String.valueOf(System.currentTimeMillis())));
        producer.setNamesrvAddr(nameserverAddressSetter.getNewNameServerAddress());
        producer.setVipChannelEnabled(false);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 消息最大大小128K
        int maxSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.MaxMsgSize, "128000"));
        producer.setMaxMessageSize(maxSize - 10 * 1000);
        return producer;
    }

    private void start() throws MQClientException {
        this.worker = new ThreadFactoryImpl("MQ-AsyncArrayDispatcher-Thread" + instanceId, true)
            .newThread(new AsyncRunnable());
        this.worker.start();
        this.traceProducer.start();
        this.registerShutDownHook();
        this.scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    String newAddrs = nameserverAddressSetter.getNewNameServerAddress();
                    if (newAddrs != null && !newAddrs.equals(traceNamesrvAddr)) {
                        CLIENT_LOG.info("trace producer update name server address , old is {}, new is {}", traceNamesrvAddr, newAddrs);
                        traceNamesrvAddr = newAddrs;
                        traceProducer.setNamesrvAddr(newAddrs);
                        traceProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
                    }
                } catch (Throwable e) {
                }
            }
        }, 30 * 1000L, 30 * 1000L, TimeUnit.MILLISECONDS);
    }

    private void shutdown() {
        flush();
        this.stopped = true;
        this.traceExecuter.shutdown();
        this.removeShutdownHook();
        this.scheduledExecutorService.shutdown();
        this.traceProducer.shutdown();
    }

    private void flush() {
        long end = System.currentTimeMillis() + 500;
        while (traceContextQueue.size() > 0 || appenderQueue.size() > 0 && System.currentTimeMillis() <= end) {
            try {
                flushTraceContext();
            } catch (Throwable e) {
            }
        }
        CLIENT_LOG.info("------end trace send " + traceContextQueue.size() + "   " + appenderQueue.size());
    }

    private void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new ThreadFactoryImpl("ShutdownHookMQTrace").newThread(new Runnable() {
                private volatile boolean hasShutdown = false;

                @Override
                public void run() {
                    synchronized (this) {
                        if (!this.hasShutdown) {
                            try {
                                flush();
                            } catch (Throwable e) {
                                CLIENT_LOG.error("system mqtrace hook shutdown failed ,maybe loss some trace data");
                            }
                        }
                    }
                }
            });
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    private void removeShutdownHook() {
        if (shutDownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutDownHook);
                shutDownHook = null;
            } catch (Throwable e) {
                CLIENT_LOG.warn("the shutdown hook already clean");
            }
        }
    }

    private void flushTraceContext() {
        List<OnsTraceContext> contexts = new ArrayList<OnsTraceContext>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            OnsTraceContext context = null;
            try {
                context = traceContextQueue.poll(5, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
            if (context != null) {
                contexts.add(context);
            } else {
                break;
            }
        }
        if (contexts.size() > 0) {
            AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
            traceExecuter.submit(request);
        }
    }

    class AsyncRunnable implements Runnable {
        private boolean stopped;

        @Override
        public void run() {
            while (!stopped) {
                try {
                    flushTraceContext();
                    if (InnerTraceProducer.this.stopped) {
                        this.stopped = true;
                    }
                } catch (Throwable e) {
                }
            }

        }
    }

    class AsyncAppenderRequest implements Runnable {
        List<OnsTraceContext> contextList;

        public AsyncAppenderRequest(final List<OnsTraceContext> contextList) {
            if (contextList != null) {
                this.contextList = contextList;
            } else {
                this.contextList = new ArrayList<OnsTraceContext>(1);
            }
        }

        @Override
        public void run() {
            sendTraceData(contextList);
        }

        /**
         * 往消息缓冲区编码轨迹数据
         *
         * @param contextList
         */
        public void sendTraceData(List<OnsTraceContext> contextList) {
            Map<String, List<OnsTraceTransferBean>> transBeanMap = new HashMap<String, List<OnsTraceTransferBean>>(16);
            String currentRegionId = null;
            for (OnsTraceContext context : contextList) {
                currentRegionId = context.getRegionId();
                if (currentRegionId == null || context.getTraceBeans().isEmpty()) {
                    continue;
                }
                String topic = context.getTraceBeans().get(0).getTopic();
                String key = topic + OnsTraceConstants.CONTENT_SPLITOR + currentRegionId;
                List<OnsTraceTransferBean> transBeanList = transBeanMap.get(key);
                if (transBeanList == null) {
                    transBeanList = new ArrayList<OnsTraceTransferBean>();
                    transBeanMap.put(key, transBeanList);
                }
                OnsTraceTransferBean traceData = OnsTraceDataEncoder.encoderFromContextBean(context);
                traceData.setBrokerSet(context.getBrokerSet());
                transBeanList.add(traceData);
            }
            for (Map.Entry<String, List<OnsTraceTransferBean>> entry : transBeanMap.entrySet()) {
                String[] key = entry.getKey().split(String.valueOf(OnsTraceConstants.CONTENT_SPLITOR));
                flushData(entry.getValue(), key[0], key[1]);
            }
        }

        /**
         * 实际批量发送数据
         */
        private void flushData(List<OnsTraceTransferBean> transBeanList, String topic, String currentRegionId) {
            if (transBeanList.size() == 0) {
                return;
            }
            // 临时缓冲区
            StringBuilder buffer = new StringBuilder(1024);
            int count = 0;
            Set<String> keySet = new HashSet<String>();
            Set<String> brokerSet = new HashSet<String>();
            for (OnsTraceTransferBean bean : transBeanList) {
                brokerSet = bean.getBrokerSet();
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
                count++;
                // 保证包的大小不要超过上限
                if (buffer.length() >= traceProducer.getMaxMessageSize()) {
                    sendTraceDataByMQ(keySet, buffer.toString(), topic, currentRegionId, brokerSet);
                    // 发送完成，清除临时缓冲区
                    buffer.delete(0, buffer.length());
                    keySet.clear();
                    count = 0;
                }
            }
            if (count > 0) {
                sendTraceDataByMQ(keySet, buffer.toString(), topic, currentRegionId, brokerSet);
            }
            transBeanList.clear();
        }

        /**
         * 发送数据的接口
         *
         * @param keySet 本批次包含的keyset
         * @param data 本批次的轨迹数据
         */
        private void sendTraceDataByMQ(Set<String> keySet, final String data, String dataTopic,
            String currentRegionId, Set<String> dataBrokerSet) {
            String topic = OnsTraceConstants.traceTopic + currentRegionId;
            final Message message = new Message(topic, data.getBytes());
            message.setKeys(keySet);
            SendCallback callback = new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                }

                @Override
                public void onException(Throwable e) {
                    //todo 对于发送失败的数据，如何保存，保证所有轨迹数据都记录下来
                    CLIENT_LOG.info("send trace data ,the traceData is " + data);
                }
            };
            try {
                if (!selectQueueEnable) {
                    traceProducer.send(message, callback, 5000);
                    return;
                }
                Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), topic);
                dataBrokerSet.retainAll(traceBrokerSet);
                if (dataBrokerSet.isEmpty()) {
                    //no cross set
                    traceProducer.send(message, callback, 5000);
                } else {
                    traceProducer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Set<String> brokerSet = (Set<String>) arg;
                            List<MessageQueue> filterMqs = new ArrayList<MessageQueue>();
                            for (MessageQueue queue : mqs) {
                                if (brokerSet.contains(queue.getBrokerName())) {
                                    filterMqs.add(queue);
                                }
                            }
                            int index = sendWhichQueue.getAndIncrement();
                            int pos = Math.abs(index) % filterMqs.size();
                            if (pos < 0) {
                                pos = 0;
                            }
                            return filterMqs.get(pos);
                        }
                    }, dataBrokerSet, callback);
                }

            } catch (Exception e) {
                CLIENT_LOG.info("send trace data,the traceData is" + data);
            }
        }

    }

    public static Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
        Set<String> brokerSet = new HashSet<String>();
        String realTopic = NamespaceUtil.wrapNamespace(producer.getDefaultMQProducer().getNamespace(), topic);
        TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(realTopic);
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            producer.getTopicPublishInfoTable().putIfAbsent(realTopic, new TopicPublishInfo());
            producer.getmQClientFactory().updateTopicRouteInfoFromNameServer(realTopic);
            topicPublishInfo = producer.getTopicPublishInfoTable().get(realTopic);
        }
        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
                brokerSet.add(queue.getBrokerName());
            }
        }
        return brokerSet;
    }

    public static Set<String> tryGetMessageQueueBrokerSet(DefaultMQPushConsumerImpl consumer, String topic) {
        Set<String> brokerSet = new HashSet<String>();
        try {
            String realTopic = NamespaceUtil.wrapNamespace(consumer.getDefaultMQPushConsumer().getNamespace(), topic);
            Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues(realTopic);
            for (MessageQueue queue : messageQueues) {
                brokerSet.add(queue.getBrokerName());
            }
        } catch (MQClientException e) {
            CLIENT_LOG.info("fetch message queue failed, the topic is {}", topic);
        }
        return brokerSet;
    }
}
