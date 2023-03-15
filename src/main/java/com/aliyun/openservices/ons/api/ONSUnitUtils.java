package com.aliyun.openservices.ons.api;

import com.google.common.base.Strings;

public class ONSUnitUtils {
    //--------多活消息特有属性键-----------
    private static final String MSG_PROP_UNIT_ORIUNIT = "__RMQ.UNIT.ORIUNIT";
    private static final String MSG_PROP_UNIT_TYPE = "__RMQ.UNIT.TYPE";
    private static final String MSG_PROP_UNIT_KEY = "__RMQ.UNIT.KEY";

    //--------消息的系统标识，MessageConst中的系统属性-----------
    private static final String PROPERTY_TRANSIENT_MSHA_RETRY = "__RMQ.TRANSIENT.MSHA_RETRY";
    private static final String PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";

    /**
     * 多活架构下，获得该多活消息的单元
     * @param msg
     * @return
     */
    public static String getUnitOriUnit(Message msg) {
        return msg.getUserProperties(MSG_PROP_UNIT_ORIUNIT);
    }

    /**
     * 多活架构下，获得消息的单元类型属性
     * @param msg
     * @return
     */
    public static String getUnitType(Message msg) {
        return msg.getUserProperties(MSG_PROP_UNIT_TYPE);
    }

    /**
     * 多活架构下，获得消息的流量标识属性，比如用户ID值等
     * @param msg
     * @return
     */
    public static String getUnitKey(Message msg) {
        return msg.getUserProperties(MSG_PROP_UNIT_KEY);
    }

    /**
     * 多活架构下，设置消息的流量信息
     *
     * @param msg
     * @param oriUnitId
     * @param unitType
     * @param unitKey
     */
    public static void setUnitProperties(Message msg, String oriUnitId, String unitType, String unitKey) {
        msg.putUserProperties(MSG_PROP_UNIT_ORIUNIT, oriUnitId);
        msg.putUserProperties(MSG_PROP_UNIT_TYPE, unitType);
        msg.putUserProperties(MSG_PROP_UNIT_KEY, unitKey);
    }

    /**
     * 是否是重试消息
     */
    public static boolean isRetryMsg(Message message) {
        return !Strings.isNullOrEmpty(message.getSystemProperties(PROPERTY_RETRY_TOPIC));
    }

    /**
     * 设置重试消息需要同步的标记
     */
    public static void setMSHAUnitRetry(Message message) {
        message.putSystemProperties(PROPERTY_TRANSIENT_MSHA_RETRY, String.valueOf(Boolean.TRUE));
    }

    public static String getMSHAUnitRetry(Message message) {
        return message.getSystemProperties(PROPERTY_TRANSIENT_MSHA_RETRY);
    }
}
