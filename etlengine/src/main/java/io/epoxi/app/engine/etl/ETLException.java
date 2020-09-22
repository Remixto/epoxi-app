package io.epoxi.app.engine.etl;

import lombok.Getter;
import io.epoxi.app.cloud.logging.BaseException;
import io.epoxi.app.cloud.logging.StatusCode;

public class ETLException extends BaseException{

    private static final long serialVersionUID = -4600540053967489069L;

    @Getter
    private final transient ETLStep step;

    ETLException (String message, ETLStep step, StatusCode statusCode)
    {
        super(message, statusCode);
        this.step = step;
    }

    ETLException (String message, Throwable cause, StatusCode statusCode)
    {
        super(message, cause, statusCode);
        this.step= null;
    }

    ETLException (String message, ETLStep step, Throwable cause, StatusCode statusCode)
    {
        super(message, cause, statusCode);
        this.step = step;
    }

}
