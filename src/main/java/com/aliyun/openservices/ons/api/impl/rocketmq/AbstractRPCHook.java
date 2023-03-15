package com.aliyun.openservices.ons.api.impl.rocketmq;

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.RPCHook;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.AccessKey;
import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.ONSChannelKey;
import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.SecurityToken;

/**
 * @author MQDevelopers
 */
public abstract class AbstractRPCHook implements RPCHook {
    protected ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]> fieldCache =
            new ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]>();


    protected SortedMap<String, String> parseRequestContent(RemotingCommand request, String ak, String securityToken, String onsChannel) {
        CommandCustomHeader header = request.readCustomHeader();
        // sort property
        SortedMap<String, String> map = new TreeMap<String, String>();
        map.put(AccessKey, ak);
        map.put(ONSChannelKey, onsChannel);
        if (securityToken != null) {
            map.put(SecurityToken, securityToken);
        }
        try {
            // add header properties
            if (null != header) {
                Field[] fields = fieldCache.get(header.getClass());
                if (null == fields) {
                    fields = header.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true);
                    }
                    Field[] tmp = fieldCache.putIfAbsent(header.getClass(), fields);
                    if (null != tmp) {
                        fields = tmp;
                    }
                }

                for (Field field : fields) {
                    Object value = field.get(header);
                    if (null != value && !field.isSynthetic()) {
                        map.put(field.getName(), value.toString());
                    }
                }
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("incompatible exception.", e);
        }
    }

}
