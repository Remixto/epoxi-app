package io.epoxi.repository.exception;

import io.epoxi.cloud.logging.BaseException;
import io.epoxi.cloud.logging.StatusCode;
import lombok.Getter;

public class NotFoundException extends BaseException {
 
    public NotFoundException(String message, Object notFoundValue) {
        super(message, StatusCode.INVALID_ARGUMENT);
        this.notFoundValue = notFoundValue;
    }

    @Getter
    final transient Object notFoundValue;

    /**
     *
     */
    private static final long serialVersionUID = 1940495467050536228L;

   
    
}