package io.epoxi.app.util.validation;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class ValidatorTest {

    @Test
    public void ValidateTrueTest() {
        Validator validator = new Validator();
        validator.add(new TestCondition(true));
        validator.add(new TestCondition(true));
        List<Violation> violations = validator.validate();

        assertTrue(violations.isEmpty());

    }

    @Test
    public void ValidateFalseTest() {
        Validator validator = new Validator();
        validator.add(new TestCondition(false));
        validator.add(new TestCondition(true));
        List<Violation> violations = validator.validate();

        assertFalse(violations.isEmpty());

    }

    @Test
    public void ValidOrThrow_Valid_Test() {
        Validator validator = new Validator();
        validator.add(new TestCondition(true));
        validator.add(new TestCondition(true));
        String name = "Valid Object";

        assertDoesNotThrow(() -> validator.validOrThrow(name));

    }

    @Test
    public void ValidOrThrow_Throw_Test() {
        Validator validator = new Validator();
        validator.add(new TestCondition(true));
        validator.add(new TestCondition(false));
        String name = "Invalid Object";

        assertThrows(InvalidStateException.class, () -> validator.validOrThrow(name));
    }

    @Getter @Setter @NonNull
    private String testVal;

    private static class TestCondition implements ValidationCondition {

        Boolean valid;

        public TestCondition(Boolean valid)
        {
            this.valid = valid;
        }

        @Override
        public Optional<String> validate() {

        if (valid)
            return Optional.empty();
        else
            return Optional.of("Invalid Condition");
        }
    }


}
