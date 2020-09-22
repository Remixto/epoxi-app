package io.epoxi.engine.etl.pipeline;

import lombok.Getter;
import io.epoxi.cloud.logging.BaseException;
import io.epoxi.cloud.logging.StatusCode;

public class PipelineException extends BaseException{



    PipelineException (String message, EtlPipeline pipeline, StatusCode statusCode)
    {
        super(message, statusCode);
        this.pipeline = pipeline;

    }

    PipelineException (String message, EtlPipeline pipeline, Throwable cause, StatusCode statusCode)
    {
        super(message, cause, statusCode);

        this.pipeline = pipeline;
    }


    private static final long serialVersionUID = -4600540053967489349L;

    @Getter
    private final transient EtlPipeline pipeline;

}
