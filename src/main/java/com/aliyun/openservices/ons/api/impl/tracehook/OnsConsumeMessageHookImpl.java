package com.aliyun.openservices.ons.api.impl.tracehook;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.ons.open.trace.core.common.OnsTraceBean;
import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceContext;
import com.alibaba.ons.open.trace.core.common.OnsTraceType;
import com.alibaba.ons.open.trace.core.dispatch.AsyncDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeExactlyOnceStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeReturnType;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.hook.ConsumeMessageHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;

public class OnsConsumeMessageHookImpl implements ConsumeMessageHook {
    /**
     * 该Hook该由哪个dispatcher发送轨迹数据
     */
    private AsyncDispatcher localDispatcher;

    public OnsConsumeMessageHookImpl(AsyncDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "OnsConsumeMessageHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        OnsTraceContext onsTraceContext = new OnsTraceContext();
        context.setMqTraceContext(onsTraceContext);
        onsTraceContext.setTraceType(OnsTraceType.SubBefore);
        String userGroup = NamespaceUtil.withoutNamespace(context.getConsumerGroup(), context.getNamespace());
        onsTraceContext.setGroupName(userGroup);
        List<OnsTraceBean> beans = new ArrayList<OnsTraceBean>();
        for (MessageExt msg : context.getMsgList()) {
            if (msg == null) {
                continue;
            }
            String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
            String traceOn = msg.getProperty(MessageConst.PROPERTY_TRACE_SWITCH);
            if (regionId == null || regionId.equals(OnsTraceConstants.default_region)) {
                // if regionId is default ,skip it
                continue;
            }
            if (traceOn != null && "false".equals(traceOn)) {
                // if trace switch is false ,skip it
                continue;
            }
            OnsTraceBean traceBean = new OnsTraceBean();

            String userTopic = NamespaceUtil.withoutNamespace(msg.getTopic(), context.getNamespace());
            traceBean.setTopic(userTopic);
            traceBean.setMsgId(msg.getMsgId());
            traceBean.setTags(msg.getTags());
            traceBean.setKeys(msg.getKeys());
            traceBean.setStoreTime(msg.getStoreTimestamp());
            traceBean.setBodyLength(msg.getStoreSize());
            traceBean.setRetryTimes(msg.getReconsumeTimes());
            onsTraceContext.setRegionId(regionId);
            beans.add(traceBean);
        }
        if (beans.size() > 0) {
            onsTraceContext.setTraceBeans(beans);
            onsTraceContext.setTimeStamp(System.currentTimeMillis());
            localDispatcher.append(onsTraceContext);
        }
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        OnsTraceContext subBeforeContext = (OnsTraceContext) context.getMqTraceContext();
        if (subBeforeContext.getRegionId().equals(OnsTraceConstants.default_region)) {
            // if regionId is default ,skip it
            return;
        }
        if (subBeforeContext.getTraceBeans() == null || subBeforeContext.getTraceBeans().size() < 1) {
            // if subbefore bean is null ,skip it
            return;
        }
        OnsTraceContext subAfterContext = new OnsTraceContext();
        subAfterContext.setTraceType(OnsTraceType.SubAfter);//
        subAfterContext.setRegionId(subBeforeContext.getRegionId());//
        subAfterContext.setGroupName(subBeforeContext.getGroupName());//
        subAfterContext.setRequestId(subBeforeContext.getRequestId());//
        subAfterContext.setSuccess(context.isSuccess());//
        // 批量消息全部处理完毕的平均耗时
        int costTime = (int) ((System.currentTimeMillis() - subBeforeContext.getTimeStamp()) / context.getMsgList().size());
        subAfterContext.setCostTime(costTime);//
        subAfterContext.setTraceBeans(subBeforeContext.getTraceBeans());
        String contextType = context.getProps().get(MixAll.CONSUME_CONTEXT_TYPE);
        if (contextType != null) {
            subAfterContext.setContextCode(ConsumeReturnType.valueOf(contextType).ordinal());
        }
        String exactlyOnceStatus = context.getProps().get(MixAll.CONSUME_EXACTLYONCE_STATUS);
        if (exactlyOnceStatus != null) {
            subAfterContext.setExactlyOnceStatus(ConsumeExactlyOnceStatus.valueOf(exactlyOnceStatus).ordinal());
        }
        localDispatcher.append(subAfterContext);
    }
}
