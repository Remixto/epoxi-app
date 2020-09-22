package io.epoxi.app.util.sqlbuilder;

import io.epoxi.app.util.sqlbuilder.statement.SqlStatement;
import io.epoxi.app.cloud.logging.AppLog;
import io.epoxi.app.cloud.logging.BaseException;
import io.epoxi.app.cloud.logging.StatusCode;

import java.lang.invoke.MethodHandles;

public class SqlBuilderException extends BaseException{

    private static final AppLog logger = new AppLog(MethodHandles.lookup().lookupClass());

    private static final long serialVersionUID = -4600540234567489349L;

    public SqlBuilderException (String message, StatusCode statusCode)
    {
        this(message, null, null, statusCode);
    }

    public SqlBuilderException (String message, SqlStatement statement, StatusCode statusCode)
    {
        this(message, statement, null, statusCode);
    }

    public SqlBuilderException (String message, Throwable cause, StatusCode statusCode)
    {
        this(message, null, cause, statusCode);
    }

    public SqlBuilderException (String message, SqlStatement statement, Throwable cause, StatusCode statusCode)
    {
        super(message, cause, statusCode);

        String statementClassName = "Statement";
        if (statement != null) statementClassName = statement.getClass().getName();
         //Log an error
         logger.atError()
         .addException(cause)
         .log("QueryBuilder encountered an error while building {}. {}", statementClassName, cause.getCause());
    }

}
