package com.aliyun.openservices.ons.api;

import java.io.Serializable;
import java.util.Properties;

/**
 * 消息类. 一条消息由主题, 消息体以及可选的消息标签, 自定义附属键值对构成.
 *
 * <p> <strong>注意:</strong> 我们对每条消息的自定义键值对的长度没有限制, 但所有的自定义键值对, 系统键值对序列化后, 所占空间不能超过32767字节. </p>
 */
public class Message implements Serializable {

    private static final long serialVersionUID = -1385924226856188094L;

    /**
     * <p> 系统属性 </p>
     */
    Properties systemProperties;

    /**
     * <p> 消息主题, 最长不超过255个字符; 由a-z, A-Z, 0-9, 以及中划线"-"和下划线"_"构成. </p>
     *
     * <p> <strong>一条合法消息本成员变量不能为空</strong> </p>
     */
    private String topic;

    /**
     * 用户属性
     */
    private Properties userProperties;

    /**
     * <p> 消息体, 消息体长度默认不超过4M, 具体请参阅集群部署文档描述. </p>
     *
     * <p> <strong>一条合法消息本成员变量不能为空</strong> </p>
     */
    private byte[] body;

    /**
     * 默认构造函数; 必要属性后续通过Set方法设置.
     */
    public Message() {
        this(null, null, "", null);
    }

    /**
     * 有参构造函数.
     *
     * @param topic 消息主题, 最长不超过255个字符; 由a-z, A-Z, 0-9, 以及中划线"-"和下划线"_"构成.
     * @param tag 消息标签, 请使用合法标识符, 尽量简短且见名知意
     * @param key 业务主键
     * @param body 消息体, 消息体长度默认不超过4M, 具体请参阅集群部署文档描述.
     */
    public Message(String topic, String tag, String key, byte[] body) {
        this.topic = topic;
        this.body = body;

        this.putSystemProperties(SystemPropKey.TAG, tag);
        this.putSystemProperties(SystemPropKey.KEY, key);
    }

    /**
     * 添加系统属性K-V对.
     *
     * @param key 属性Key
     * @param value 属性值
     */
    void putSystemProperties(final String key, final String value) {
        if (null == this.systemProperties) {
            this.systemProperties = new Properties();
        }

        if (key != null && value != null) {
            this.systemProperties.put(key, value);
        }
    }

    /**
     * 消息有参构造函数
     *
     * @param topic 消息主题, 最长不超过255个字符; 由a-z, A-Z, 0-9, 以及中划线"-"和下划线"_"构成.
     * @param tags 消息标签, 合法标识符, 尽量简短且见名知意
     * @param body 消息体, 消息体长度默认不超过4M, 具体请参阅集群部署文档描述.
     */
    public Message(String topic, String tags, byte[] body) {
        this(topic, tags, "", body);
    }

    /**
     * 添加用户自定义属性键值对; 该键值对在消费消费时可被获取.
     *
     * @param key 自定义键
     * @param value 对应值
     */
    public void putUserProperties(final String key, final String value) {
        if (null == this.userProperties) {
            this.userProperties = new Properties();
        }

        if (key != null && value != null) {
            this.userProperties.put(key, value);
        }
    }

    /**
     * 获取用户自定义键的值
     *
     * @param key 自定义键
     * @return 用户自定义键值
     */
    public String getUserProperties(final String key) {
        if (null != this.userProperties) {
            return (String) this.userProperties.get(key);
        }

        return null;
    }

    /**
     * 获取消息的主题
     *
     * @return 消息主题.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 设置消息主题.
     *
     * @param topic 消息主题
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * 获取消息标签
     *
     * @return 消息标签
     */
    public String getTag() {
        return this.getSystemProperties(SystemPropKey.TAG);
    }

    /**
     * 获取系统键的值
     *
     * @param key 预定义的系统键
     * @return 指定系统键的值
     */
    String getSystemProperties(final String key) {
        if (null != this.systemProperties) {
            return this.systemProperties.getProperty(key);
        }

        return null;
    }

    /**
     * 设置消息标签
     *
     * @param tag 标签.
     */
    public void setTag(String tag) {
        this.putSystemProperties(SystemPropKey.TAG, tag);
    }

    /**
     * 获取业务码
     *
     * @return 业务码
     */
    public String getKey() {
        return this.getSystemProperties(SystemPropKey.KEY);
    }

    /**
     * 设置业务码
     *
     * @param key 业务码
     */
    public void setKey(String key) {
        this.putSystemProperties(SystemPropKey.KEY, key);
    }

    /**
     * 获取消息ID
     *
     * @return 该消息ID
     */
    public String getMsgID() {
        return this.getSystemProperties(SystemPropKey.MSGID);
    }

    /**
     * 设置该消息ID
     *
     * @param msgid 该消息ID.
     */
    public void setMsgID(String msgid) {
        this.putSystemProperties(SystemPropKey.MSGID, msgid);
    }

    Properties getSystemProperties() {
        return systemProperties;
    }

    void setSystemProperties(Properties systemProperties) {
        this.systemProperties = systemProperties;
    }

    public Properties getUserProperties() {
        return userProperties;
    }

    public void setUserProperties(Properties userProperties) {
        this.userProperties = userProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    /**
     * 消息消费时, 获取消息已经被重试消费的次数
     *
     * @return 重试消费次数.
     */
    public int getReconsumeTimes() {
        String pro = this.getSystemProperties(SystemPropKey.RECONSUMETIMES);
        if (pro != null) {
            return Integer.parseInt(pro);
        }

        return 0;
    }

    /**
     * 设置消息重试消费次数.
     *
     * @param value 重试消费次数.
     */
    public void setReconsumeTimes(final int value) {
        putSystemProperties(SystemPropKey.RECONSUMETIMES, String.valueOf(value));
    }

    /**
     * 获取消息的生产时间
     *
     * @return 消息的生产时间.
     */
    public long getBornTimestamp() {
        String pro = this.getSystemProperties(SystemPropKey.BORNTIMESTAMP);
        if (pro != null) {
            return Long.parseLong(pro);
        }

        return 0L;
    }

    /**
     * 设置消息的产生时间.
     *
     * @param value 消息生产时间.
     */
    public void setBornTimestamp(final long value) {
        putSystemProperties(SystemPropKey.BORNTIMESTAMP, String.valueOf(value));
    }

    /**
     * 获取产生消息的主机.
     *
     * @return 产生消息的主机
     */
    public String getBornHost() {
        String pro = this.getSystemProperties(SystemPropKey.BORNHOST);
        return pro == null ? "" : pro;
    }

    /**
     * 设置生产消息的主机
     *
     * @param value 生产消息的主机
     */
    public void setBornHost(final String value) {
        putSystemProperties(SystemPropKey.BORNHOST, value);
    }

    /**
     * 获取定时消息开始投递时间.
     *
     * @return 定时消息的开始投递时间.
     */
    public long getStartDeliverTime() {
        String pro = this.getSystemProperties(SystemPropKey.STARTDELIVERTIME);
        if (pro != null) {
            return Long.parseLong(pro);
        }

        return 0L;
    }

    public String getShardingKey() {
        String pro = this.getSystemProperties(SystemPropKey.SHARDINGKEY);
        return pro == null ? "" : pro;
    }

    public void setShardingKey(final String value) {
        putSystemProperties(SystemPropKey.SHARDINGKEY, value);
    }

    /**
     * <p> 设置消息的定时投递时间（绝对时间),最大延迟时间为7天. </p> <ol> <li>延迟投递: 延迟3s投递, 设置为: System.currentTimeMillis() + 3000;</li>
     * <li>定时投递: 2016-02-01 11:30:00投递, 设置为: new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2016-02-01
     * 11:30:00").getTime()</li> </ol>
     */
    public void setStartDeliverTime(final long value) {
        putSystemProperties(SystemPropKey.STARTDELIVERTIME, String.valueOf(value));
    }

    /**
     * @return 该消息在所属 Partition 里的偏移量
     */
    public long getOffset() {
        String v = getSystemProperties(SystemPropKey.CONSUMEOFFSET);
        if (v != null) {
            return Long.parseLong(v);
        }
        return 0L;
    }

    /**
     * @return 该消息所属的 Partition
     */
    public TopicPartition getTopicPartition() {
        return new TopicPartition(topic, getSystemProperties(SystemPropKey.PARTITION));
    }

    @Override
    public String toString() {
        return "Message [topic=" + topic + ", systemProperties=" + systemProperties + ", userProperties=" + userProperties + ", body="
            + (body != null ? body.length : 0) + "]";
    }

    /**
     * 该类预定义一些系统键.
     */
    static public class SystemPropKey {
        public static final String TAG = "__TAG";
        public static final String KEY = "__KEY";
        public static final String MSGID = "__MSGID";
        public static final String SHARDINGKEY = "__SHARDINGKEY";
        public static final String RECONSUMETIMES = "__RECONSUMETIMES";
        public static final String BORNTIMESTAMP = "__BORNTIMESTAMP";
        public static final String BORNHOST = "__BORNHOST";
        /**
         * 设置消息的定时投递时间（绝对时间). <p>例1: 延迟投递, 延迟3s投递, 设置为: System.currentTimeMillis() + 3000; <p>例2: 定时投递, 2016-02-01
         * 11:30:00投递, 设置为: new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2016-02-01 11:30:00").getTime()
         */
        public static final String STARTDELIVERTIME = "__STARTDELIVERTIME";

        public static final String CONSUMEOFFSET = "__CONSUMEOFFSET";

        public static final String PARTITION = "__PARTITION";
    }
}
