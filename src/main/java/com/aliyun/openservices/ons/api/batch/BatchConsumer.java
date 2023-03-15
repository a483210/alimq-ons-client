package com.aliyun.openservices.ons.api.batch;

import com.aliyun.openservices.ons.api.Admin;

/**
 * 批量消息消费者，用来通过批量的方式订阅消息
 */
public interface BatchConsumer extends Admin {

    /**
     * 订阅消息
     *
     * @param topic 消息主题
     * @param subExpression 订阅过滤表达式字符串，ONS服务器依据此表达式进行过滤。只支持或运算<br> eg: "tag1 || tag2 || tag3"<br>
     * 如果subExpression等于null或者*，则表示全部订阅
     * @param listener 消息回调监听器
     */
    void subscribe(final String topic, final String subExpression, final BatchMessageListener listener);

    /**
     * 取消某个topic订阅
     *
     * @param topic 消息主题
     */
    void unsubscribe(final String topic);
}
