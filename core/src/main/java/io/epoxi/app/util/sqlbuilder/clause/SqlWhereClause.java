package io.epoxi.app.util.sqlbuilder.clause;

import io.epoxi.app.util.sqlbuilder.types.SqlPredicate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlWhereClause implements SqlClause {

    final List<SqlPredicate> predicates = new ArrayList<>();

    public SqlWhereClause (SqlPredicate... expressions)
    {
        Collections.addAll(predicates, expressions);
    }

    public String getSql()
    {
        List<String> strings = new ArrayList<>();

        predicates.forEach( x -> strings.add(x.getSql()));

        return String.format("WHERE     %s", String.join("\nAND ", strings));

    }

}
