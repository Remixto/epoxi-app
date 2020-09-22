package io.epoxi.util.sqlbuilder.types;

public class SqlFieldPair {

    SqlField field1;
    SqlField field2;

    public SqlFieldPair (SqlTable table1, SqlTable table2, String fieldName)
    {
        this.field1 = table1.getField(fieldName);
        this.field2 = table2.getField(fieldName);
    }

    public SqlFieldPair (SqlField field1, SqlField field2)
    {
        this.field1 = field1;
        this.field2 = field2;
    }

    public SqlField getField1() {
        return field1;
    }

    public void setField1(SqlField field1) {
        this.field1 = field1;
    }

    public SqlField getField2() {
        return field2;
    }

    public void setField2(SqlField field2) {
        this.field2 = field2;
    }

    public static SqlFieldPairBuilder newBuilder()
    {
        return new SqlFieldPairBuilder();
    }

     /********************************************************
     * Predicate Builder
     *******************************************************/
    public static class SqlFieldPairBuilder
    {
        SqlField field1;
        SqlField field2;

        public SqlFieldPairBuilder setField1(SqlField sqlField)
        {
            this.field1 = sqlField;
            return this;
        }

        public SqlFieldPairBuilder setField2(SqlField sqlField)
        {
            this.field2 = sqlField;
            return this;
        }

        public SqlFieldPair build()
        {
            return new SqlFieldPair(field1, field2);
        }
    }

}
