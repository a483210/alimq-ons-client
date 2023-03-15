package com.aliyun.openservices.ons.api.impl.namespace;

import java.util.regex.Pattern;

public class InstanceUtil {
    public static final String SEPARATOR_V1 = "_";
    public static final String REGEX_INSTANCE_V1 = "MQ_INST_\\w+_\\w{8}";
    public static final Pattern PATTERN_INSTANCE_V1 = Pattern.compile("^" + REGEX_INSTANCE_V1);

    public static final String SEPARATOR_V2 = "-";
    public static final String REGEX_INSTANCE_V2 = "mqi-\\w+-\\w+";
    public static final Pattern PATTERN_INSTANCE_V2 = Pattern.compile("^" + REGEX_INSTANCE_V2);

    public static boolean validateInstanceId(String instanceId) {
        return instanceIdVersion(instanceId) > 0;
    }

    private static int instanceIdVersion(String instanceId) {
        if (PATTERN_INSTANCE_V1.matcher(instanceId).matches()) {
            return 1;
        }

        if (PATTERN_INSTANCE_V2.matcher(instanceId).matches()) {
            return 2;
        }

        return 0;
    }

    public static boolean isIndependentNaming(String instanceId) {
        if (instanceIdVersion(instanceId) == 2) {
            String[] arr = instanceId.split(SEPARATOR_V2);
            if (arr != null && arr.length >= 3 && arr[2] != null && arr[2].length() > 3) {
                return (arr[2].charAt(3) & 0x1) == 1;
            }
        }
        return true;
    }
}
