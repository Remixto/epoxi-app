package io.epoxi.app.util.validation;

import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

/***
 * A test of a ValidationCondition.  The test may be enabled or disabled.
 */
public class ValidationTest {
    @Getter @Setter
    ValidationCondition condition;

    @Getter @Setter
    private Boolean enabled;

    private ValidationTest(ValidationCondition condition, Boolean enabled)
    {
        this.condition = condition;
        this.enabled = enabled;
    }

	public static ValidationTest of(ValidationCondition condition, Boolean enabled) {

        return new ValidationTest(condition, enabled);
	}

	public Optional<String> validate() {
        if (Boolean.FALSE.equals(enabled))
            return Optional.empty();

        return condition.validate();
	}
}
