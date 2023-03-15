package com.aliyun.openservices.ons.api.spi;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.google.common.base.Optional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultInvocationContext implements InvocationContext {
    private List<Message> messages;

    private SendResult sendResult;

    private ONSClientException exception;

    private Action action;

    private OrderAction orderAction;

    private String consumerGroup;

    private String namespaceId;

    private final Map<String, Object> attributes;

    public DefaultInvocationContext() {
        attributes = new HashMap<String, Object>();
    }

    @Override
    public Optional<List<Message>> getMessages() {
        if (null == messages) {
            return Optional.absent();
        }
        return Optional.of(messages);
    }

    @Override
    public Optional<SendResult> getSendResult() {
        if (null == sendResult) {
            return Optional.absent();
        }
        return Optional.of(sendResult);
    }

    @Override
    public Optional<ONSClientException> getException() {
        if (null == exception) {
            return Optional.absent();
        }
        return Optional.of(exception);
    }

    @Override
    public Optional<Action> getAction() {
        if (null == action) {
            return Optional.absent();
        }
        return Optional.of(action);
    }

    @Override
    public void setAction(Action action) {
        this.action = action;
    }

    @Override
    public Optional<OrderAction> getOrderAction() {
        if (null == orderAction) {
            return Optional.absent();
        }
        return Optional.of(orderAction);
    }

    @Override
    public void setOrderAction(OrderAction orderAction) {
        this.orderAction = orderAction;
    }

    @Override
    public Optional<String> getConsumerGroup() {
        if (null == consumerGroup) {
            return Optional.absent();
        }
        return Optional.of(consumerGroup);
    }

    @Override
    public Optional<String> getNamespaceId() {
        if (null == namespaceId) {
            return Optional.absent();
        }
        return Optional.of(namespaceId);
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void setNamespaceId(String namespaceId) {
        this.namespaceId = namespaceId;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }

    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }

    public void setException(ONSClientException exception) {
        this.exception = exception;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }
}
