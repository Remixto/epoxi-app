package io.epoxi.app.util.sqlbuilder.clause;

/**
 * Used exclusively in the MERGE statement
 */
public class SqlWhenMatchedClause implements SqlClause {

    final SqlUpdateSetClause updateClause;

    public SqlWhenMatchedClause ( SqlUpdateSetClause updateClause)
    {
        this.updateClause = updateClause;
    }

    public String getSql()
    {
        return String.format("WHEN MATCHED THEN%nUPDATE %s", updateClause.getSql());
    }

}
