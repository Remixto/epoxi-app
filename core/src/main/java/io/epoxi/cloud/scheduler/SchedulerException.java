package io.epoxi.cloud.scheduler;

import io.epoxi.cloud.logging.BaseException;
import io.epoxi.cloud.logging.StatusCode;

public class SchedulerException extends BaseException {

    private static final long serialVersionUID = -4900120021967489323L;

    SchedulerException (String message, StatusCode statusCode)
    {
        super(message, statusCode);
    }

    SchedulerException (String message, Throwable cause, StatusCode statusCode)
    {
        super(message, cause,  statusCode);
    }
}
