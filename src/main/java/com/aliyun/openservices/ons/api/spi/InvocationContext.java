package com.aliyun.openservices.ons.api.spi;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.google.common.base.Optional;

import java.util.List;
import java.util.Map;

public interface InvocationContext {

    Optional<List<Message>> getMessages();

    Optional<SendResult> getSendResult();

    Optional<ONSClientException> getException();

    Optional<Action> getAction();

    void setAction(Action action);

    Optional<OrderAction> getOrderAction();

    void setOrderAction(OrderAction orderAction);

    Optional<String> getConsumerGroup();

    Optional<String> getNamespaceId();

    Map<String, Object> getAttributes();
}
