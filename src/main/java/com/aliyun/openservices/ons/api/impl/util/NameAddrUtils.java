package com.aliyun.openservices.ons.api.impl.util;

import java.util.regex.Pattern;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;
import org.apache.commons.lang3.StringUtils;

public class NameAddrUtils {
    public static final String INSTANCE_REGEX_V1 = "MQ_INST_\\w+_\\w+";
    public static final String INSTANCE_REGEX_V2 = "mqi-\\w+-\\w+";

    public static final Pattern NAMESRV_ENDPOINT_PATTERN = Pattern.compile("^(\\w+://|).*");
    public static final Pattern INST_ENDPOINT_PATTERN_V1 = Pattern.compile("^(\\w+://|)" + INSTANCE_REGEX_V1 + "\\..*");
    public static final Pattern INST_ENDPOINT_PATTERN_V2 = Pattern.compile("^(\\w+://|)" + INSTANCE_REGEX_V2 + "\\..*");

    public static String getNameAdd() {
        return System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    }

    public static boolean validateInstanceEndpoint(String endpoint) {
        return INST_ENDPOINT_PATTERN_V1.matcher(endpoint).matches() || INST_ENDPOINT_PATTERN_V2.matcher(endpoint).matches();
    }

    public static String parseInstanceIdFromEndpoint(String endpoint) {
        if (StringUtils.isEmpty(endpoint)) {
            return null;
        }
        return endpoint.substring(endpoint.lastIndexOf('/') + 1, endpoint.indexOf('.'));
    }

    public static String parseNameSrvAddrFromNamesrvEndpoint(String nameSrvEndpoint) {
        if (StringUtils.isEmpty(nameSrvEndpoint)) {
            return "";
        }
        return nameSrvEndpoint.substring(nameSrvEndpoint.lastIndexOf('/') + 1);
    }
}
