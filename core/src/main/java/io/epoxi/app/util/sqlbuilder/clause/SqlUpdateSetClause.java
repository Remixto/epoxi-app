package io.epoxi.app.util.sqlbuilder.clause;

import io.epoxi.app.util.sqlbuilder.types.SqlFieldPair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlUpdateSetClause implements SqlClause {

    final List<SqlFieldPair> fieldPairs = new ArrayList<>();

    public SqlUpdateSetClause(SqlFieldPair... expressions)
    {
        Collections.addAll(fieldPairs, expressions);
    }

    public SqlUpdateSetClause( List<SqlFieldPair> pairs)
    {
        fieldPairs.addAll(pairs);
    }

    public String getSql()
    {
        List<String> strings = new ArrayList<>();

        fieldPairs.forEach(x -> strings.add(String.format("      %s = %s", x.getField1().getPath(), x.getField2().getPrefixPath()))
        );

        return String.format( "SET%n      %s", String.join(",\n      ", strings));

    }



}
