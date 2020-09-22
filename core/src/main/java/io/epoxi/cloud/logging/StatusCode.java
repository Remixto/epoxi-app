package io.epoxi.cloud.logging;

public enum StatusCode {
    OK(200),
    CANCELLED(499),
    UNKNOWN(500),
    INVALID_ARGUMENT(400),
    DEADLINE_EXCEEDED(504),
    NOT_FOUND(404),
    ALREADY_EXISTS(409),
    PERMISSION_DENIED(403),
    RESOURCE_EXHAUSTED(429),
    FAILED_PRECONDITION(400),
    ABORTED(409),
    OUT_OF_RANGE(400),
    UNIMPLEMENTED(501),
    INTERNAL(500),
    UNAVAILABLE(503),
    DATA_LOSS(500),
    UNAUTHENTICATED(401);

    private final int code;

    StatusCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static StatusCode of(com.google.api.gax.rpc.StatusCode apiExceptionStatusCode)
    {
        return Enum.valueOf(StatusCode.class, apiExceptionStatusCode.getCode().name());
    }
}
