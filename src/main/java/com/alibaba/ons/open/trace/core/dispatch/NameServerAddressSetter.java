package com.alibaba.ons.open.trace.core.dispatch;

public interface NameServerAddressSetter {
    /**
     * 更新 NameServer 列表
     * @return
     */
    String getNewNameServerAddress();
}
