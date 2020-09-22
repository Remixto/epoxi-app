package io.epoxi.util.sqlbuilder.types;

import lombok.NonNull;

public class SqlPredicate {

    private final SqlField leftField;
    private final SqlField rightField;
    private final String operator;

    public SqlPredicate(@NonNull SqlField leftField, @NonNull String operator, @NonNull SqlField rightField)
    {
        this.leftField = leftField;
        this.rightField = rightField;
        this.operator = operator;
    }

    public String getSql()
    {
        return String.format("%s %s %s", leftField.getPrefixPath(), operator, rightField.getPrefixPath());
    }

    public SqlField getLeftField() {
        return leftField;
    }

    public SqlField getRightField() {
        return rightField;
    }

    public static SqlPredicateBuilder newBuilder()
    {
        return new SqlPredicateBuilder();
    }


    /********************************************************
     * Predicate Builder
     *******************************************************/
    public static class SqlPredicateBuilder
    {
        private SqlField leftField;
        private SqlField rightField;
        private String operator;

        public SqlPredicateBuilder setLeftField(SqlField sqlField)
        {
            this.leftField = sqlField;
            return this;
        }

        public SqlPredicateBuilder setRightField(SqlField sqlField)
        {
            this.rightField = sqlField;
            return this;
        }

        public SqlPredicateBuilder setFieldPair(SqlFieldPair sqlFieldPair)
        {
            this.leftField = sqlFieldPair.field1;
            this.rightField = sqlFieldPair.field2;
            this.operator = "=";

            return this;
        }

        public SqlPredicateBuilder setOperator(String operator)
        {
            this.operator = operator;
            return this;
        }

        public SqlPredicate build()
        {
            return new SqlPredicate(leftField, operator, rightField);
        }

    }



}
