package com.aliyun.openservices.ons.api.impl.auth;

/**
 * @author mq-develop
 * @createTime 06
 * @description
 */
public class StsCredentials {

    private String ramRoleName;

    private String accessKeyId;

    private String accessKeySecret;

    private long expiration;

    private String securityToken;

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    public String getSecurityToken() {
        return securityToken;
    }

    public void setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
    }

    public String getRamRoleName() {
        return ramRoleName;
    }

    public void setRamRoleName(String ramRoleName) {
        this.ramRoleName = ramRoleName;
    }
}
