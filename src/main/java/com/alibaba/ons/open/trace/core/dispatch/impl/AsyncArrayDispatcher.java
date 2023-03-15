package com.alibaba.ons.open.trace.core.dispatch.impl;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceContext;
import com.alibaba.ons.open.trace.core.common.OnsTraceDispatcherType;
import com.alibaba.ons.open.trace.core.dispatch.AsyncDispatcher;
import com.alibaba.ons.open.trace.core.dispatch.NameServerAddressSetter;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RPCHook;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentialsProvider;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static com.alibaba.ons.open.trace.core.dispatch.impl.InnerTraceProducer.tryGetMessageQueueBrokerSet;

/**
 * @author MQDevelopers
 */
public class AsyncArrayDispatcher implements AsyncDispatcher {
    private final boolean innerProducerSingleton;
    private final InnerTraceProducer traceProducer;
    private String dispatcherType;
    private DefaultMQProducerImpl hostProducer;
    private DefaultMQPushConsumerImpl hostConsumer;
    private String dispatcherId = UUID.randomUUID().toString();

    public AsyncArrayDispatcher(Properties properties, SessionCredentialsProvider provider,
        final NameServerAddressSetter nameserverAddressSetter) throws MQClientException {
        dispatcherType = properties.getProperty(OnsTraceConstants.TraceDispatcherType);
        innerProducerSingleton = Boolean.parseBoolean(properties.getProperty(OnsTraceConstants.TraceProducerSingleton, "true"));
        traceProducer = InnerTraceProducer.getTraceDispatcherProducer(properties, provider, nameserverAddressSetter);
    }

    public AsyncArrayDispatcher(Properties properties,
        final NameServerAddressSetter nameserverAddressSetter, RPCHook rpcHook) throws MQClientException {
        dispatcherType = properties.getProperty(OnsTraceConstants.TraceDispatcherType);
        innerProducerSingleton = Boolean.parseBoolean(properties.getProperty(OnsTraceConstants.TraceProducerSingleton, "true"));
        traceProducer = InnerTraceProducer.getTraceDispatcherProducer(properties, nameserverAddressSetter, rpcHook);
    }

    public DefaultMQProducerImpl getHostProducer() {
        return hostProducer;
    }

    public void setHostProducer(DefaultMQProducerImpl hostProducer) {
        this.hostProducer = hostProducer;
    }

    public DefaultMQPushConsumerImpl getHostConsumer() {
        return hostConsumer;
    }

    public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
        this.hostConsumer = hostConsumer;
    }

    @Override
    public void start() throws MQClientException {
        InnerTraceProducer.registerTraceDispatcher(dispatcherId, traceProducer, innerProducerSingleton);
    }

    @Override
    public boolean append(final OnsTraceContext ctx) {
        if (ctx.getTraceBeans().isEmpty()) {
            return true;
        }
        String topic = ctx.getTraceBeans().get(0).getTopic();
        Set<String> brokerSet = getBrokerSetByTopic(topic);
        ctx.setBrokerSet(brokerSet);
        return traceProducer.append(ctx);
    }
    @Override
    public void shutdown() {
        InnerTraceProducer.unregisterTraceDispatcher(dispatcherId, traceProducer, innerProducerSingleton);
    }

    private Set<String> getBrokerSetByTopic(String topic) {
        Set<String> brokerSet = new HashSet<String>();
        if (dispatcherType != null && dispatcherType.equals(OnsTraceDispatcherType.PRODUCER.name()) && hostProducer != null) {
            brokerSet = tryGetMessageQueueBrokerSet(hostProducer, topic);
        }
        if (dispatcherType != null && dispatcherType.equals(OnsTraceDispatcherType.CONSUMER.name()) && hostConsumer != null) {
            brokerSet = tryGetMessageQueueBrokerSet(hostConsumer, topic);
        }
        return brokerSet;
    }
}
