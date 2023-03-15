package com.aliyun.openservices.ons.api.impl.authority.exception;

public class SignatureException extends RuntimeException {

    private static final long serialVersionUID = -3662598055526208602L;
    private final int code;


    public SignatureException(int code) {
        super();
        this.code = code;
    }


    public SignatureException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }


    public SignatureException(int code, String message) {
        super(message);
        this.code = code;
    }


    public SignatureException(int code, Throwable cause) {
        super(cause);
        this.code = code;
    }
}
