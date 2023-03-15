package com.aliyun.openservices.ons.api.impl.authority;

/**
 * Provider to fetch {@link SessionCredentials}.
 */
public interface SessionCredentialsProvider {
    /**
     * Get {@link SessionCredentials}.
     *
     * @return session credentials.
     */
    SessionCredentials getSessionCredentials();
}
