package com.aliyun.openservices.ons.api;

/**
 * 该类存储常用量值.
 */
public class PropertyValueConst {

    /**
     * 广播消费模式
     */
    public static final String BROADCASTING = "BROADCASTING";

    /**
     * 集群消费模式
     */
    public static final String CLUSTERING = "CLUSTERING";

    /**
     * 订阅方是否是使用循环平均分配策略
     */
    public static final String ALLOCATE_MESSAGE_QUEUE_CIRCLE = "AVG_BY_CIRCLE";

    /**
     * 订阅方是否是使用启发式平均分配策略
     */
    public static final String ALLOCATE_MESSAGE_QUEUE_HEURISTIC = "AVG_BY_HEURISTIC";
}
