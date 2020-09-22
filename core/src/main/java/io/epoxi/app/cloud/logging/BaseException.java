package io.epoxi.app.cloud.logging;

import lombok.Getter;

public abstract class BaseException extends RuntimeException {

    public BaseException (String message, StatusCode statusCode)
    {
        super(message);
        this.statusCode = statusCode;
    }

    public BaseException (String message, Throwable cause, StatusCode statusCode)
    {
        super(message, cause);
        this.statusCode = statusCode;
    }

    /**
     *
     */
    private static final long serialVersionUID = -5867468426896058565L;

    @Getter
    private final StatusCode statusCode;
}
