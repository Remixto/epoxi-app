package io.epoxi.util.sqlbuilder.clause;

import io.epoxi.util.sqlbuilder.statement.SelectStatement;
import io.epoxi.util.sqlbuilder.types.SqlPredicate;

/**
 * Used exclusively in the MERGE statement
 */
public class SqlUsingClause implements SqlClause {

    final SqlPredicate predicate;
    final SelectStatement selectStatement;
    final String statementAlias;

    public SqlUsingClause ( SelectStatement selectStatement, String statementAlias, SqlPredicate predicate)
    {
        this.selectStatement = selectStatement;
        this.statementAlias = statementAlias;
        this.predicate = predicate;
    }

    public String getSql()
    {
        return String.format("USING     (%n%s%n) %s %nON %s", selectStatement.getSql(false), statementAlias, predicate.getSql());
    }

}
