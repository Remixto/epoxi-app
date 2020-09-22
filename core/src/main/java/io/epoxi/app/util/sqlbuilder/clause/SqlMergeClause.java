package io.epoxi.app.util.sqlbuilder.clause;

import io.epoxi.app.util.sqlbuilder.statement.SqlStatement;
import io.epoxi.app.util.sqlbuilder.types.SqlTable;

public class SqlMergeClause implements SqlClause {

    final SqlTable table;
    final SqlStatement statement;

    public SqlMergeClause (SqlStatement statement, SqlTable table)
    {
        this.statement = statement;
        this.table = table;
    }

    public String getSql()
    {
        return String.format("MERGE     %s", table.getQualifiedNameAlias());
    }

}
