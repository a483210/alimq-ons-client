package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import com.aliyun.openservices.ons.api.impl.authority.AuthUtil;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentials;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentialsProvider;

import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.AccessKey;
import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.ONSChannelKey;
import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.SecurityToken;
import static com.aliyun.openservices.ons.api.impl.authority.SessionCredentials.Signature;

/**
 * @author MQDevelopers
 */
public class ClientRPCHook extends AbstractRPCHook {
    protected SessionCredentialsProvider provider;

    public ClientRPCHook(SessionCredentialsProvider provider) {
        this.provider = provider;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        final SessionCredentials sessionCredentials = provider.getSessionCredentials();
        byte[] total = AuthUtil.combineRequestContent(request,
            parseRequestContent(request, sessionCredentials.getAccessKey(),
                sessionCredentials.getSecurityToken(), sessionCredentials.getOnsChannel().name()));
        String signature = AuthUtil.calSignature(total, sessionCredentials.getSecretKey());
        request.addExtField(Signature, signature);
        request.addExtField(AccessKey, sessionCredentials.getAccessKey());
        request.addExtField(ONSChannelKey, sessionCredentials.getOnsChannel().name());

        if (sessionCredentials.getSecurityToken() != null) {
            request.addExtField(SecurityToken, sessionCredentials.getSecurityToken());
        }

        request.setNamespaceId(sessionCredentials.getNamespaceId());
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

    @Override
    public void doAfterRpcFailure(String remoteAddr, RemotingCommand request, Boolean remoteTimeout) {

    }

}
