package io.epoxi.app.util.sqlbuilder.statement;

import io.epoxi.app.util.sqlbuilder.clause.*;
import io.epoxi.util.sqlbuilder.clause.*;
import io.epoxi.app.util.sqlbuilder.types.RowField;
import io.epoxi.app.util.sqlbuilder.types.SqlFieldPair;
import io.epoxi.app.util.sqlbuilder.types.SqlPredicate;
import io.epoxi.app.util.sqlbuilder.types.SqlTable;

import java.util.List;


public class UpdateStatement  extends SqlStatement implements SqlClause {

    public static UpdateStatement update(SqlTable table)
    {
        UpdateStatement statement = new UpdateStatement();
        SqlUpdateClause update = new SqlUpdateClause(table);
        statement.clauses.add(update);

        return statement;
    }

    public UpdateStatement set(List<SqlFieldPair> pairs)
    {
        SqlUpdateSetClause update = new SqlUpdateSetClause(pairs);
        clauses.add(update);

        return this;
    }

    public UpdateStatement from(SqlTable table)
    {
        tables.add(table);
        SqlFromClause from = new SqlFromClause(table);
        clauses.add(from);

        return this;
    }

    public UpdateStatement innerJoin(SqlTable table, String left, String right)
    {
       return join(SqlJoinClause.JoinType.INNER, table, left, right);
    }

    public UpdateStatement rightJoin(SqlTable table, String left, String right)
    {
        return join(SqlJoinClause.JoinType.LEFT, table, left, right);

    }

    public UpdateStatement leftJoin(SqlTable table, String left, String right)
    {
        return join(SqlJoinClause.JoinType.RIGHT, table, left, right);
    }


    public UpdateStatement fullJoin(SqlTable table, String left, String right)
    {
        return join(SqlJoinClause.JoinType.FULL, table, left, right);
    }

    private UpdateStatement join(SqlJoinClause.JoinType type, SqlTable table, String left, String right)
    {
        tables.add(table);
        RowField leftField = tables.get(0).getField(left);
        RowField rightField = table.getField(right);

        SqlJoinClause clause = SqlJoinClause.of(type, leftField, rightField);
        clauses.add(clause);

        return this;
    }

    public UpdateStatement where(SqlPredicate... expressions)
    {
        SqlWhereClause where = new SqlWhereClause(expressions);
        clauses.add(where);

        return this;
    }


}
