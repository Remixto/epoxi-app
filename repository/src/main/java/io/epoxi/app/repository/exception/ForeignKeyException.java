package io.epoxi.app.repository.exception;

import io.epoxi.app.cloud.logging.BaseException;
import io.epoxi.app.cloud.logging.StatusCode;

public class ForeignKeyException extends BaseException {
 
    public ForeignKeyException(String message) {
        super(message, StatusCode.INVALID_ARGUMENT);

    }
  

    /**
     *
     */
    private static final long serialVersionUID = 6240423467050536228L;

   
    
}