package com.aliyun.openservices.ons.api.impl.rocketmq;

public class FAQ {
    public static final String FIND_NS_FAILED =
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&namesrv_not_exist";

    public static final String CONNECT_BROKER_FAILED =
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&connect_broker_failed";

    public static final String SEND_MSG_TO_BROKER_TIMEOUT =
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&send_msg_failed";

    public static final String SERVICE_STATE_WRONG =
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&service_not_ok";

    public static final String BROKER_RESPONSE_EXCEPTION =
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&broker_response_exception";

    public static final String CLIENT_CHECK_MSG_EXCEPTION =
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&msg_check_failed";

    public static final String TOPIC_ROUTE_NOT_EXIST =
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&topic_not_exist";


    public static String errorMessage(final String msg, final String url) {
        return String.format("%s\nSee %s for further details.", msg, url);
    }
}
