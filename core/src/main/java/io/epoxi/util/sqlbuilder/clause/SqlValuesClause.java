package io.epoxi.util.sqlbuilder.clause;

import io.epoxi.util.sqlbuilder.types.RowField;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap;

import java.util.ArrayList;
import java.util.List;

public class SqlValuesClause implements SqlClause {

    final List<String> fields = new ArrayList<>();

    public SqlValuesClause( RowField... expressions)
    {
        for(RowField f : expressions)
        {
            fields.add(f.getName());
        }
    }

    public SqlValuesClause(SqlFieldMap<String, ?> fieldList)
    {
        fields.addAll(fieldList.keySet());
    }

    public String getSql()
    {
        List<String> strings = new ArrayList<>();
        fields.forEach(x -> strings.add(String.format("      %s", x)));

        return String.format( "VALUES (%n      %s)", String.join(",\n      ", strings));

    }



}
