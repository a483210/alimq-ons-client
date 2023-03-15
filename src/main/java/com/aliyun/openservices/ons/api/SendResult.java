package com.aliyun.openservices.ons.api;

/**
 * 发送结果.
 */
public class SendResult {

    /**
     * 已发送消息的ID
     */
    private String messageId;

    /**
     * 已发送消息的主题
     */
    private String topic;


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

    /**
     * 序列化发送结果.
     *
     * @return 发送结果的String表示.
     */
    @Override
    public String toString() {
        return "SendResult[topic=" + topic + ", messageId=" + messageId + ']';
    }
}
