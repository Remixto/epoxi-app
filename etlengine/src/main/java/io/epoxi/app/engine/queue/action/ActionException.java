package io.epoxi.app.engine.queue.action;

import com.google.pubsub.v1.PubsubMessage;

import io.epoxi.app.cloud.logging.BaseException;
import io.epoxi.app.cloud.logging.StatusCode;

public class ActionException extends BaseException {

    private static final long serialVersionUID = -4600540053967489323L;

    private final PubsubMessage pubsubMessage;

    public ActionException(String message, StatusCode statusCode) {
        super(message, statusCode);
        this.pubsubMessage = null;
    }

    public ActionException(String message, Throwable cause, StatusCode statusCode) {
        super(message, cause, statusCode);
        this.pubsubMessage = null;
    }

    public ActionException(String message, PubsubMessage pubsubMessage, Throwable cause, StatusCode statusCode)
    {
        super(message, cause, statusCode);
        this.pubsubMessage = pubsubMessage;
    }

    public PubsubMessage getPubsubMessage()
    {
        return pubsubMessage;
    }


}
