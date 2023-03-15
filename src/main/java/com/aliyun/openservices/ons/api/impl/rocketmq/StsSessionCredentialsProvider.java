package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.alibaba.ons.open.trace.core.utils.JsonUtils;
import com.aliyun.openservices.ons.api.impl.auth.StsCredentials;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentials;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentialsProvider;
import com.aliyun.openservices.ons.api.impl.util.ClientLoggerUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.utils.HttpTinyClient;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;
import com.fasterxml.jackson.databind.JsonNode;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StsSessionCredentialsProvider implements SessionCredentialsProvider {
    private static final String URL_ECS_RAM_ROLE = "http://100.100.100.200/latest/meta-data/Ram/security-credentials/";
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();

    private static final long DEFAULT_REFRESH_INTERVAL_MILLIS = 30L * 60 * 60L * 1000L;
    private static final long MIN_REFRESH_INTERVAL_MILLIS = 5000L;
    private static final long HTTP_TIMEOUT_MILLIS = 3000L;

    private final ONSClientAbstract client;
    private final String ramRoleName;
    private final SessionCredentials sessionCredentials;

    private final ScheduledExecutorService scheduledExecutorService =
            new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("ONSRamRoleStsTokenUpdater"));

    public StsSessionCredentialsProvider(ONSClientAbstract client, String ramRoleName) {
        this.client = client;
        this.ramRoleName = ramRoleName;
        this.sessionCredentials = new SessionCredentials(client.sessionCredentials);
        refreshToken(ramRoleName);
    }

    private void refreshToken(String ramRoleName) {
        StsCredentials stsCredentials = fetchStsCredentialsByRamRole(ramRoleName);
        if (stsCredentials != null) {
            sessionCredentials.setAccessKey(stsCredentials.getAccessKeyId());
            sessionCredentials.setSecretKey(stsCredentials.getAccessKeySecret());
            sessionCredentials.setSecurityToken(stsCredentials.getSecurityToken());
            // Replace sessionCredentials in ons client.
            client.sessionCredentials = sessionCredentials;
            LOGGER.info("update sts credentials success, ramRoleName={}", ramRoleName);
            dispatchTokenRefreshTask(stsCredentials);
        } else {
            dispatchTokenRefreshTask(ramRoleName, MIN_REFRESH_INTERVAL_MILLIS);
        }
    }

    private void dispatchTokenRefreshTask(final StsCredentials stsCredentials) {
        long now = System.currentTimeMillis();
        long interval = stsCredentials.getExpiration() - now;
        long delay = interval > DEFAULT_REFRESH_INTERVAL_MILLIS ?
                stsCredentials.getExpiration() - now - DEFAULT_REFRESH_INTERVAL_MILLIS :
                Math.max(MIN_REFRESH_INTERVAL_MILLIS, (stsCredentials.getExpiration() - now) / 2);
        dispatchTokenRefreshTask(stsCredentials.getRamRoleName(), delay);
    }

    private void dispatchTokenRefreshTask(final String ramRoleName, long delay) {
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    refreshToken(ramRoleName);
                } catch (Throwable t) {
                    LOGGER.error("Unexpected exception raised while refreshing sts token, ramRoleName={}",
                            ramRoleName, t);
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private StsCredentials fetchStsCredentialsByRamRole(String ramRoleName) {
        try {
            HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(URL_ECS_RAM_ROLE + ramRoleName,
                    null, null, "UTF-8", HTTP_TIMEOUT_MILLIS);
            if (200 != result.code || null == result.content || "".equals(result.content)) {
                LOGGER.error("fetch sts credentials by ram role[ " + ramRoleName + " ] error. please set right ram role");
            }
            JsonNode jsonNode = JsonUtils.mapper.readTree(result.content);

            StsCredentials stsCredentials = new StsCredentials();
            stsCredentials.setRamRoleName(ramRoleName);
            stsCredentials.setAccessKeyId(jsonNode.get("AccessKeyId").asText());
            stsCredentials.setAccessKeySecret(jsonNode.get("AccessKeySecret").asText());
            stsCredentials.setSecurityToken(jsonNode.get("SecurityToken").asText());

            SimpleDateFormat utcFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            utcFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            stsCredentials.setExpiration(utcFormatter.parse(jsonNode.get("Expiration").asText()).getTime());

            return stsCredentials;
        } catch (Exception e) {
            LOGGER.error("fetch sts credentials by ram role[ " + ramRoleName + " ] error", e);
        }
        return null;
    }

    @Override
    public SessionCredentials getSessionCredentials() {
        return client.sessionCredentials;
    }
}
