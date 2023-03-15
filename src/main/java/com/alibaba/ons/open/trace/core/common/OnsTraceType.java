package com.alibaba.ons.open.trace.core.common;

/**
 * @author MQDevelopers
 */
public enum OnsTraceType {
    /**
     * 消息发送
     */
    Pub,
    /**
     * 消息开始消费
     */
    SubBefore,
    /**
     * 消息消费完成
     */
    SubAfter,
}
