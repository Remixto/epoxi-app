package io.epoxi.cloud.bq;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.QueryParameterValue;

import io.epoxi.cloud.logging.BaseException;
import io.epoxi.cloud.logging.StatusCode;
import io.epoxi.cloud.logging.AppLog;

import java.util.Map;

public class BqException extends BaseException{

    private static final AppLog logger = new AppLog(BqService.class);

    private static final long serialVersionUID = -4600540053967489064L;

    private final String query;
    private final Map<String, QueryParameterValue> parameters;

    public BqException (String message, String query, StatusCode statusCode)
    {
        this(message, query, null, null, statusCode);
    }

    public BqException (String message, String query, Map<String, QueryParameterValue> parameters, StatusCode statusCode)
    {
        this(message, query, parameters, null, statusCode);
    }

    public BqException (String message, Throwable cause, StatusCode statusCode)
    {
        this(message, null, null, cause, statusCode);
    }

    public BqException (String message, String query, Throwable cause, StatusCode statusCode)
    {
        this(message, query, null, cause, statusCode);
    }

    public BqException (String message, String query,  Map<String, QueryParameterValue> parameters, Throwable cause, StatusCode statusCode)
    {
        super(message, cause, statusCode);
        this.query = query;
        this.parameters = parameters;

        String detailMessage;

        if (cause.getClass().equals(BigQueryException.class))
        {
            BigQueryException ex = (BigQueryException)cause;
            detailMessage = ex.getLocalizedMessage();
        }
        else
        {
            detailMessage = cause.getMessage();
        }
        //Log an error
        logger.atError()
            .addException(cause)
            .addKeyValue("query", query)
            .log("{}. {}", message, detailMessage);
    }

    public String getQuery()
    {
        return query;
    }
    public Map<String, QueryParameterValue> getParameters()
    {
        return parameters;
    }
}
