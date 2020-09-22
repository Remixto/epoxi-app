package io.epoxi.app.util.sqlbuilder.clause;

public class SqlCodeClause implements SqlClause {

    private final String sql;

    public SqlCodeClause(String sql)
    {
        this.sql = sql;
    }

    @Override
    public String getSql() {
        return sql;
    }

}
