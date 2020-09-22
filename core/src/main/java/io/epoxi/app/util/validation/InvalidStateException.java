package io.epoxi.app.util.validation;

import io.epoxi.app.cloud.logging.BaseException;
import io.epoxi.app.cloud.logging.StatusCode;
import lombok.Getter;

import java.util.List;

public class InvalidStateException extends BaseException {

    public InvalidStateException(List<Violation> violations, String memberName) {
        super("Object is in an invalid state. Call getViolations() for details", StatusCode.INVALID_ARGUMENT);
        this.memberName = memberName;
        this.violations = violations;
    }

    @Getter
    private final String memberName;

    @Getter
    final transient List<Violation> violations;

    /**
     *
     */
    private static final long serialVersionUID = 6240495467050536228L;

}
