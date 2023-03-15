package com.aliyun.openservices.ons.api;

import java.util.Properties;


public class MessageAccessor {
    public static Properties getSystemProperties(final Message msg) {
        return msg.systemProperties;
    }


    public static void setSystemProperties(final Message msg, Properties systemProperties) {
        msg.systemProperties = systemProperties;
    }


    public static void putSystemProperties(final Message msg, final String key, final String value) {
        msg.putSystemProperties(key, value);
    }


    public static String getSystemProperties(final Message msg, final String key) {
        return msg.getSystemProperties(key);
    }
}
