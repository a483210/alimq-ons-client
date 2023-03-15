package com.aliyun.openservices.ons.api.impl;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MQVersion;

import java.io.InputStream;
import java.util.Properties;

public class MQClientInfo {

    public static int versionCode = MQVersion.CURRENT_VERSION;

    static {
        try {
            InputStream stream = MQClientInfo.class.getClassLoader().getResourceAsStream("ons_client_info.properties");
            Properties properties = new Properties();
            properties.load(stream);
            String pkgVersion = String.valueOf(properties.get("version"));
            versionCode = Integer.MAX_VALUE - Integer.valueOf(pkgVersion.replaceAll("[^0-9]", ""));
        } catch (Exception ignore) {
        }
    }

}
