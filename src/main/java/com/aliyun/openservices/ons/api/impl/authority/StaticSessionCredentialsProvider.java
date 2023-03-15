package com.aliyun.openservices.ons.api.impl.authority;

/**
 * Static provider which get {@link SessionCredentials} in advance.
 */
public class StaticSessionCredentialsProvider implements SessionCredentialsProvider {
    private final SessionCredentials sessionCredentials;

    public StaticSessionCredentialsProvider(SessionCredentials sessionCredentials) {
        this.sessionCredentials = sessionCredentials;
    }

    @Override
    public SessionCredentials getSessionCredentials() {
        return sessionCredentials;
    }
}
