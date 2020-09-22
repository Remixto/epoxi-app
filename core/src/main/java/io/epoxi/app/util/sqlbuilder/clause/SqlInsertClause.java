package io.epoxi.app.util.sqlbuilder.clause;

import io.epoxi.app.util.sqlbuilder.statement.SqlStatement;
import io.epoxi.app.util.sqlbuilder.types.RowField;
import io.epoxi.app.util.sqlbuilder.types.SqlFieldMap;

import java.util.ArrayList;
import java.util.List;

public class SqlInsertClause implements SqlClause {

    final List<String> fields = new ArrayList<>();
    final SqlStatement statement;

    public SqlInsertClause(SqlStatement statement, RowField... exprs)
    {
        this.statement = statement;
        for(RowField f : exprs)
        {
            fields.add(f.getName());
        }
    }

    public SqlInsertClause(SqlStatement statement, SqlFieldMap<String, ?> fieldlist)
    {
        this.statement = statement;
        fields.addAll(fieldlist.keySet());
    }

    public String getSql()
    {
        List<String> strings = new ArrayList<>();
        fields.forEach(x -> strings.add(String.format("      %s", x)));
        return String.format( "INSERT (%n      %s)", String.join(",\n      ", strings));

    }



}
