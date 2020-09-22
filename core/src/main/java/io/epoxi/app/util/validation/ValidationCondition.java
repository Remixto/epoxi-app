package io.epoxi.app.util.validation;

import java.util.Optional;

/***
 * A method that evaluations a specific validate condition and returns an error string when the condition is found to be invalid
 */
@FunctionalInterface
 public interface ValidationCondition {
    Optional<String> validate();
}


