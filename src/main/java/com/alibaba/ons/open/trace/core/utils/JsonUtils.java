package com.alibaba.ons.open.trace.core.utils;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLoggerFactory;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.exception.JSONException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.ByteBufferInputStream;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * JsonUtils
 *
 * @author Created by gold on 2023/3/14 10:14
 * @since 1.0.0
 */
public final class JsonUtils {
    private JsonUtils() {
    }

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    public static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new KeyJacksonModule())
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            //忽略空属性
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            //忽略未知属性
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            //忽略双引号
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

    public static byte[] encode(final Object obj) {
        final String json = toJson(obj, false);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static String toJson(Object obj) {
        return toJson(obj, false);
    }

    public static String toJson(Object obj, boolean prettyFormat) {
        try {
            if (prettyFormat) {
                return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
            } else {
                return mapper.writeValueAsString(obj);
            }
        } catch (JsonProcessingException e) {
            throw new JSONException(e);
        }
    }

    public static <T> T decode(final ByteBuffer data, Class<T> classOfT) {
        try {
            return mapper.readValue(new ByteBufferInputStream(data), classOfT);
        } catch (IOException e) {
            LOGGER.error("Failed to deserialize data to JSON object", e);
        }
        return null;
    }

    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data, StandardCharsets.UTF_8);
        return readValue(json, classOfT);
    }

    public static <T> T readValue(String json, Class<T> classOfT) {
        try {
            return mapper.readValue(json, classOfT);
        } catch (JsonProcessingException e) {
            throw new JSONException(e);
        }
    }

    public static <T> List<T> readArray(String json, Class<T> classOfT) {
        try {
            JavaType type = mapper.getTypeFactory().constructParametricType(List.class, classOfT);

            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new JSONException(e);
        }
    }

    public static JsonNode readTree(Reader reader) {
        try {
            return mapper.readTree(reader);
        } catch (IOException e) {
            throw new JSONException(e);
        }
    }

    public static <T> T treeToValue(TreeNode node, Class<T> valueType) {
        try {
            return mapper.treeToValue(node, valueType);
        } catch (JsonProcessingException e) {
            throw new JSONException(e);
        }
    }

    public static class KeyJacksonModule extends SimpleModule {
        public KeyJacksonModule() {
            addKeyDeserializer(MessageQueue.class, new MessageQueue.MessageQueueKeyDeserializer());
            addKeySerializer(MessageQueue.class, new MessageQueue.MessageQueueKeySerializer());
        }
    }
}
