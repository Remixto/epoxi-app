package io.epoxi.cloud.pubsub;

import io.epoxi.cloud.logging.BaseException;
import io.epoxi.cloud.logging.StatusCode;

public class PubsubException extends BaseException{

    private static final long serialVersionUID = -4900540021967489323L;

    PubsubException (String message, StatusCode statusCode)
    {
        super(message, statusCode);
    }

    PubsubException (String message, Throwable cause, StatusCode statusCode)
    {
        super(message, cause,  statusCode);
    }
}
