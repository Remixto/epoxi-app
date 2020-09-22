package io.epoxi.app.util.validation;

import lombok.Getter;

public class Violation {

    @Getter
    final String message;

    @Getter
    final ValidationCondition condition;

    public Violation(String message, ValidationCondition condition)
    {
        this.message = message;
        this.condition = condition;
    }
}
