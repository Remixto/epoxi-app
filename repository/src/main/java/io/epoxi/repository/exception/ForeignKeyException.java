package io.epoxi.repository.exception;

import io.epoxi.cloud.logging.BaseException;
import io.epoxi.cloud.logging.StatusCode;

public class ForeignKeyException extends BaseException {
 
    public ForeignKeyException(String message) {
        super(message, StatusCode.INVALID_ARGUMENT);

    }
  

    /**
     *
     */
    private static final long serialVersionUID = 6240423467050536228L;

   
    
}