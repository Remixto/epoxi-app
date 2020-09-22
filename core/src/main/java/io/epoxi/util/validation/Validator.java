package io.epoxi.util.validation;

import lombok.AccessLevel;
import lombok.Getter;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Validator extends AbstractList<ValidationTest>{

    @Getter(AccessLevel.PROTECTED)
    private final List<ValidationTest> tests = new ArrayList<>();

    public List<Violation> validate()
    {
        final List<Violation> violations = new ArrayList<>();

        for (final ValidationTest test : tests)
        {
            final Optional<String> error = test.validate();
            error.ifPresent(s -> violations.add(new Violation(s, test.getCondition())));
        }

        return violations;
    }

    /**
     * Throws InvalidStateException if validation fails
     */
    public void validOrThrow(String objectName)
    {
        if (tests.isEmpty()) return;

        List<Violation> violations = validate();
        if(!violations.isEmpty()) throw new InvalidStateException(violations, objectName);
    }

    public void add(ValidationCondition condition) {
        add(condition, true);
    }

    public void add(ValidationCondition condition, Boolean enabled) {
        add (ValidationTest.of(condition, enabled));
    }

    /***
     * Create new tests based on the list of ValidationConditions that can be found in the validator parameter
     * Each new test will be created using the enabled status specified.
     * @param validator the Validator to check for ValidationConditions
     * @param enabled If true, then the condition will be tested when the Validate method is called.
     */
    public void addConditions(Validator validator, Boolean enabled) {
        for(ValidationTest test : validator)
        {
            add(test.getCondition(), enabled);
        }
    }

    @Override
    public ValidationTest get(int index) {
       return tests.get(index);
    }

    @Override
    public void add(int index, ValidationTest element)
    {
        tests.add(element);
    }

    @Override
    public int size() {
        return tests.size();
    }

	public void setEnabled(Boolean enabled) {
        for(ValidationTest test : this)
        {
            test.setEnabled(enabled);
        }
	}





}

