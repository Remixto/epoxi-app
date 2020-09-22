package io.epoxi.engine;

import io.epoxi.cloud.logging.BaseException;
import io.epoxi.cloud.logging.StatusCode;

public class EngineException extends BaseException {

    private static final long serialVersionUID = -4600540053967489349L;


    public EngineException (String message, StatusCode statusCode)
    {
        super(message, statusCode);

    }

    public EngineException (String message, Throwable cause, StatusCode statusCode)
    {
        super(message, cause, statusCode);

    }

}
