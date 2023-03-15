package com.aliyun.openservices.ons.api;

import com.aliyun.openservices.ons.api.exception.ONSClientException;

/**
 * 发送消息异常上下文.
 */
public class OnExceptionContext {

    /**
     * 消息ID
     */
    private String messageId;

    /**
     * 消息主题
     */
    private String topic;

    /**
     * 异常对象, 包含详细的栈信息
     */
    private ONSClientException exception;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public ONSClientException getException() {
        return exception;
    }

    public void setException(ONSClientException exception) {
        this.exception = exception;
    }
}
