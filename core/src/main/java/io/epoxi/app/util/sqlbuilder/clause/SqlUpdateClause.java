package io.epoxi.app.util.sqlbuilder.clause;

import io.epoxi.app.util.sqlbuilder.types.SqlTable;

public class SqlUpdateClause implements SqlClause {

    final SqlTable table;

    public SqlUpdateClause (SqlTable table)
    {
        this.table = table;
    }

    public String getSql()
    {
        return String.format("UPDATE     %s", table.getQualifiedName());
    }

}
