package io.epoxi.app.util.sqlbuilder.clause;

import com.google.cloud.bigquery.TableId;

import io.epoxi.app.util.sqlbuilder.types.RowField;
import io.epoxi.app.util.sqlbuilder.types.SqlPredicate;
import io.epoxi.app.util.sqlbuilder.types.SqlTable;

public class SqlJoinClause implements SqlClause{

    private final JoinType joinType;
    private final SqlPredicate predicate;
    private final RowField leftField;
    private final RowField rightField;

    public SqlJoinClause (JoinType joinType, SqlPredicate predicate)
    {
        this.joinType = joinType;
        this.predicate = predicate;

        try
        {
            leftField = (RowField) predicate.getLeftField();
            rightField = (RowField) predicate.getRightField();
        }
        catch(Exception ex)
        {
            throw new IllegalArgumentException("Invalid predicate. The left and right fields of the predicate passed must of type 'RowField'");
        }
    }

    public String getSql()
    {
        StringBuilder sb = new StringBuilder();

        // INNER JOIN TableB b on a.ID = b.ID  --WHERE a table previously specified in the from clause

        //If either of the left field is a RowField, grab the table
        SqlTable rightTable = rightField.getTable();
        TableId rightTableId = rightTable.getTableId();

        String qualifiedTableAlias = SqlTable.toQualifiedNameAlias(rightTableId.getProject(), rightTableId.getDataset(), rightTableId.getTable(), rightTable.getAlias());
        sb.append(String.format("   %s JOIN %s", joinType.toString(), qualifiedTableAlias));
        sb.append(String.format(" ON %s", predicate.getSql()));

        return sb.toString();
    }

    public static SqlJoinClause of(JoinType joinType, RowField leftField, RowField rightField)
    {
        SqlPredicate predicate = new SqlPredicate(leftField, "=", rightField);
        return new SqlJoinClause(joinType, predicate);
    }

    public RowField getLeftField() {
        return leftField;
    }

    public RowField getRightField() {
        return rightField;
    }

    public enum JoinType {INNER, LEFT, RIGHT, FULL}



}
