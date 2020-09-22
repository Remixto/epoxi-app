package io.epoxi.app.util.sqlbuilder.clause;

import io.epoxi.app.util.sqlbuilder.types.SqlTable;

public class SqlFromClause implements SqlClause {

    final SqlTable table;

    public SqlFromClause (SqlTable table)
    {
        this.table = table;
    }

    public String getSql()
    {
        return String.format("FROM      %s", table.getQualifiedNameAlias());
    }

}
