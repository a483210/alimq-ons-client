package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import com.aliyun.openservices.ons.api.impl.MQClientInfo;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentialsProvider;

public class OnsClientRPCHook extends ClientRPCHook {

    public OnsClientRPCHook(SessionCredentialsProvider provider) {
        super(provider);
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        super.doBeforeRequest(remoteAddr, request);
        request.setVersion(MQClientInfo.versionCode);
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        super.doAfterResponse(remoteAddr, request, response);
    }
}
