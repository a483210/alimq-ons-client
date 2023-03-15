package com.aliyun.openservices.ons.api.impl.tracehook;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.CommunicationMode;
import java.util.ArrayList;

import com.alibaba.ons.open.trace.core.common.OnsTraceBean;
import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceContext;
import com.alibaba.ons.open.trace.core.common.OnsTraceType;
import com.alibaba.ons.open.trace.core.dispatch.AsyncDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.SendMessageContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.SendMessageHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;

public class OnsClientSendMessageHookImpl implements SendMessageHook {
    /**
     * 该Hook该由哪个dispatcher发送轨迹数据
     */
    private AsyncDispatcher localDispatcher;

    public OnsClientSendMessageHookImpl(AsyncDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "OnsClientSendMessageHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        // 如果是消息轨迹本身的发送链路，则不需要再记录
        if (context == null || context.getMessage().getTopic().startsWith(MixAll.SYSTEM_TOPIC_PREFIX)) {
            return;
        }
        OnsTraceContext onsContext = new OnsTraceContext();
        onsContext.setTraceBeans(new ArrayList<OnsTraceBean>(1));
        context.setMqTraceContext(onsContext);
        onsContext.setTraceType(OnsTraceType.Pub);
        String userGroup = NamespaceUtil.withoutNamespace(context.getProducerGroup(), context.getNamespace());
        onsContext.setGroupName(userGroup);
        OnsTraceBean traceBean = new OnsTraceBean();
        String userTopic = NamespaceUtil.withoutNamespace(context.getMessage().getTopic(), context.getNamespace());
        traceBean.setTopic(userTopic);
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setBodyLength(context.getMessage().getBody().length);
        traceBean.setMsgType(context.getMsgType());
        onsContext.getTraceBeans().add(traceBean);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        // 如果是消息轨迹本身的发送链路，则不需要再记录
        if (context == null || context.getMessage().getTopic().startsWith(OnsTraceConstants.traceTopic) || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }
        if (context.getSendResult().getRegionId() == null
            || context.getSendResult().getRegionId().equals(OnsTraceConstants.default_region)
            || !context.getSendResult().isTraceOn()) {
            // if regionId is default or switch is false,skip it
            return;
        }
        OnsTraceContext onsContext = (OnsTraceContext) context.getMqTraceContext();
        if (context.getCommunicationMode().equals(CommunicationMode.ASYNC)) {
            onsContext.setCostTime(-1);
        } else {
            int costTime = (int) ((System.currentTimeMillis() - onsContext.getTimeStamp()) / onsContext.getTraceBeans().size());
            onsContext.setCostTime(costTime);
        }
        if (context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK)) {
            onsContext.setSuccess(true);
        } else {
            onsContext.setSuccess(false);
        }
        onsContext.setRegionId(context.getSendResult().getRegionId());
        OnsTraceBean traceBean = onsContext.getTraceBeans().get(0);
        traceBean.setMsgId(context.getSendResult().getMsgId());
        traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
        traceBean.setStoreTime(onsContext.getTimeStamp());
        localDispatcher.append(onsContext);
    }
}
