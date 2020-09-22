package io.epoxi.app.util.sqlbuilder.clause;
/**
 * Used exclusively in the MERGE statement
 */
public class SqlWhenNotMatchedBySourceClause implements SqlClause {

    final SqlUpdateSetClause updateClause;

    public SqlWhenNotMatchedBySourceClause (SqlUpdateSetClause updateClause)
    {
        this.updateClause = updateClause;
    }

    public String getSql()
    {
        return String.format("WHEN NOT MATCHED BY SOURCE THEN%nUPDATE %s", updateClause.getSql());
    }

}
