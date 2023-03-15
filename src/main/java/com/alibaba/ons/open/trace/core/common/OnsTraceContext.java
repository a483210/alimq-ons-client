package com.alibaba.ons.open.trace.core.common;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageClientIDSetter;
import java.util.List;
import java.util.Set;

/**
 * @author MQDevelopers
 */
public class OnsTraceContext implements Comparable<OnsTraceContext> {
    /**
     * 轨迹数据的类型，Pub,SubBefore,SubAfter
     */
    private OnsTraceType traceType;
    /**
     * 记录时间
     */
    private long timeStamp = System.currentTimeMillis();
    /**
     * Region信息
     */
    private String regionId = "";
    private String regionName = "";
    /**
     * 发送组或者消费组名
     */
    private String groupName = "";
    /**
     * 耗时，单位ms
     */
    private int costTime = 0;
    /**
     * 消费状态，成功与否
     */
    private boolean isSuccess = true;
    /**
     * UUID,用于匹配消费前和消费后的数据
     */
    private String requestId = MessageClientIDSetter.createUniqID();
    /**
     * context状态
     */
    private int contextCode = 0;
    /**
     * exactlyOnce状态
     */
    private int exactlyOnceStatus = 0;
    /**
     * 针对每条消息的轨迹数据
     */
    private List<OnsTraceBean> traceBeans;

    private Set<String> brokerSet;

    public int getContextCode() {
        return contextCode;
    }

    public void setContextCode(final int contextCode) {
        this.contextCode = contextCode;
    }

    public int getExactlyOnceStatus() {
        return exactlyOnceStatus;
    }

    public void setExactlyOnceStatus(int exactlyOnceStatus) {
        this.exactlyOnceStatus = exactlyOnceStatus;
    }

    public List<OnsTraceBean> getTraceBeans() {
        return traceBeans;
    }


    public void setTraceBeans(List<OnsTraceBean> traceBeans) {
        this.traceBeans = traceBeans;
    }


    public String getRegionId() {
        return regionId;
    }


    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }


    public OnsTraceType getTraceType() {
        return traceType;
    }


    public void setTraceType(OnsTraceType traceType) {
        this.traceType = traceType;
    }


    public long getTimeStamp() {
        return timeStamp;
    }


    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }


    public String getGroupName() {
        return groupName;
    }


    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }


    public int getCostTime() {
        return costTime;
    }


    public void setCostTime(int costTime) {
        this.costTime = costTime;
    }


    public boolean isSuccess() {
        return isSuccess;
    }


    public void setSuccess(boolean success) {
        isSuccess = success;
    }


    public String getRequestId() {
        return requestId;
    }


    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public Set<String> getBrokerSet() {
        return brokerSet;
    }

    public void setBrokerSet(Set<String> brokerSet) {
        this.brokerSet = brokerSet;
    }

    @Override
    public int compareTo(OnsTraceContext o) {
        return (int) (this.timeStamp - o.getTimeStamp());
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(1024);
        sb.append(traceType).append("_").append(groupName)
            .append("_").append(regionId).append("_").append(isSuccess).append("_");
        if (traceBeans != null && traceBeans.size() > 0) {
            for (OnsTraceBean bean : traceBeans) {
                sb.append(bean.getMsgId() + "_" + bean.getTopic() + "_");
            }
        }
        return "OnsTraceContext{" + sb.toString() + '}';
    }
}
