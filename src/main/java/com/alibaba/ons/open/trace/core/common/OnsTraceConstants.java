package com.alibaba.ons.open.trace.core.common;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;

import javax.annotation.Generated;

/**
 * @author MQDevelopers
 */
@Generated("ons-client")
public class OnsTraceConstants {
    /**
     * 外部直传Nameserver的地址
     */
    public static final String NAMESRV_ADDR = "NAMESRV_ADDR";
    /**
     * 外部传入地址服务器的Url，获取NameServer地址
     */
    public static final String ADDRSRV_URL = "ADDRSRV_URL";
    /**
     * 外部传入AK，SK构建hook
     */
    public static final String AccessKey = "AccessKey";
    /**
     * sk
     */
    public static final String SecretKey = "SecretKey";
    /**
     * 实例名称
     */
    public static final String InstanceName = "InstanceName";
    /**
     * 缓冲区队列大小
     */
    public static final String AsyncBufferSize = "AsyncBufferSize";
    /**
     * 最大Batch
     */
    public static final String MaxBatchNum = "MaxBatchNum";

    public static final String WakeUpNum = "WakeUpNum";
    /**
     * Batch消息最大大小
     */
    public static final String MaxMsgSize = "MaxMsgSize";

    /**
     * producer名称
     */
    public static final String groupName = "_INNER_TRACE_PRODUCER";
    /**
     * topic
     */
    public static final String traceTopic = MixAll.SYSTEM_TOPIC_PREFIX + "TRACE_DATA_";

    /**
     * topic
     */
    public static final String default_region = MixAll.DEFAULT_TRACE_REGION_ID;

    public static final char CONTENT_SPLITOR = (char) 1;
    public static final char FIELD_SPLITOR = (char) 2;

    public static final String TraceDispatcherType = "DispatcherType";

    public static final String TraceProducerSingleton = "TraceProducerSingleton";

    public static final String MsgTraceSelectQueueEnable = "MsgTraceSelectQueueEnable";
}
