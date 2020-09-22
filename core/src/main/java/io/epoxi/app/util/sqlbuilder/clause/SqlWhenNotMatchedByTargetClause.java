package io.epoxi.app.util.sqlbuilder.clause;

import io.epoxi.app.util.sqlbuilder.statement.InsertStatement;

/**
 * Used exclusively in the MERGE statement
 */
public class SqlWhenNotMatchedByTargetClause implements SqlClause {

    final InsertStatement insertStatement;

    public SqlWhenNotMatchedByTargetClause (InsertStatement insertStatement)
    {
        this.insertStatement = insertStatement;
    }

    public String getSql()
    {
        return String.format("WHEN NOT MATCHED BY TARGET THEN%n%s", insertStatement.getSql(false));
    }

}
