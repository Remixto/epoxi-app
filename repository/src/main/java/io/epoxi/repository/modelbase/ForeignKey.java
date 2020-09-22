package io.epoxi.repository.modelbase;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.ElementType;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ForeignKey {

    Class<?> value();
    ConstraintAction action() default ConstraintAction.DISALLOW;

    enum ConstraintAction
    {
        CASCADE,
        DISALLOW
    }

}
