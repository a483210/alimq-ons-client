package com.aliyun.openservices.ons.api.impl.authority;

import java.util.Map;
import java.util.SortedMap;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.CHARSET;

/**
 * @author MQDevelopers
 */
public class AuthUtil {
    public static byte[] combineRequestContent(RemotingCommand request, SortedMap<String, String> fieldsMap) {
        try {
            StringBuilder sb = new StringBuilder("");
            for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
                if (!SessionCredentials.Signature.equals(entry.getKey())) {
                    sb.append(entry.getValue());
                }
            }

            return AuthUtil.combineBytes(sb.toString().getBytes(CHARSET), request.getByteArrayBody());
        } catch (Exception e) {
            throw new RuntimeException("incompatible exception.", e);
        }
    }


    public static byte[] combineBytes(byte[] b1, byte[] b2) {
        int size = (null != b1 ? b1.length : 0) + (null != b2 ? b2.length : 0);
        byte[] total = new byte[size];
        if (null != b1) {
            System.arraycopy(b1, 0, total, 0, b1.length);
        }
        if (null != b2) {
            System.arraycopy(b2, 0, total, b1.length, b2.length);
        }
        return total;
    }


    public static String calSignature(byte[] data, String secretKey) {
        String signature = OnsAuthSigner.calSignature(data, secretKey);
        return signature;
    }
}
