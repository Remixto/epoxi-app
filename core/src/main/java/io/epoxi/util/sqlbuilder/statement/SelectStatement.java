package io.epoxi.util.sqlbuilder.statement;

import io.epoxi.util.sqlbuilder.clause.*;
import io.epoxi.util.sqlbuilder.types.*;
import io.epoxi.util.sqlbuilder.clause.*;
import io.epoxi.util.sqlbuilder.types.*;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap.FieldMapBuilder;

import java.util.List;

public class SelectStatement extends SqlStatement implements SqlClause {

    private SqlFieldMap<String, SqlField> selectFields;

    public static SelectStatement select(RowField... expressions)
    {
        SelectStatement statement = new SelectStatement();
        SqlSelectClause select = new SqlSelectClause(expressions);
        statement.clauses.add(select);

        return statement;
    }

    public static SelectStatement select(SqlFieldMap<String, ? extends SqlField> sqlFieldMap)
    {
        SelectStatement statement = new SelectStatement();
        SqlSelectClause select = new SqlSelectClause(sqlFieldMap);
        statement.clauses.add(select);

        statement.selectFields = select.getSelectFields();

        return statement;
    }

    public static SelectStatement select(SqlSelectClause clause)
    {
        SelectStatement statement = new SelectStatement();
        statement.clauses.add(clause);

        return statement;
    }

    public SelectStatement from(SqlTable table)
    {
        tables.add(table);
        SqlFromClause from = new SqlFromClause(table);
        clauses.add(from);

        return this;
    }

    public SelectStatement innerJoin(SqlTable table, String left, String right)
    {
       return join(SqlJoinClause.JoinType.INNER, table, left, right);
    }

    public SelectStatement rightJoin(SqlTable table, String left, String right)
    {
        return join(SqlJoinClause.JoinType.LEFT, table, left, right);

    }

    public SelectStatement leftJoin(SqlTable table, String left, String right)
    {
        return join(SqlJoinClause.JoinType.RIGHT, table, left, right);
    }


    public SelectStatement fullJoin(SqlTable table, String left, String right)
    {
        return join(SqlJoinClause.JoinType.FULL, table, left, right);
    }

    private SelectStatement join(SqlJoinClause.JoinType type, SqlTable table, String left, String right)
    {
        tables.add(table);
        RowField leftField = tables.get(0).getField(left);
        RowField rightField = table.getField(right);

        SqlJoinClause clause = SqlJoinClause.of(type, leftField, rightField);
        clauses.add(clause);

        return this;
    }

    private SelectStatement join(SqlJoinClause join)
    {
        SqlTable rightTable = join.getRightField().getTable();
        tables.add(rightTable);

        //If the tables inside the left and right fields have no alias, assign one
        if(rightTable.getAlias(false) == null)
        {
            rightTable.setAlias(String.format("t%d", tables.size()));
        }
        clauses.add(join);

        return this;
    }

    public SelectStatement where(SqlPredicate... expressions)
    {
        SqlWhereClause where = new SqlWhereClause(expressions);
        clauses.add(where);

        return this;
    }

    public SelectStatement groupBy(RowField... expressions)
    {
        SqlGroupByClause groupBy = new SqlGroupByClause(expressions);
        clauses.add(groupBy);

        return this;
    }

    public SelectStatement groupBy(SqlFieldMap<String, SqlField> sqlFieldMap)
    {
        SqlGroupByClause groupBy = new SqlGroupByClause(sqlFieldMap);
        clauses.add(groupBy);

        return this;
    }

    public SqlFieldMap<String, SqlField> getSelectFields()
    {
        return selectFields;
    }

    public SelectStatement code(String code)
    {
        SqlCodeClause groupBy = new SqlCodeClause(code);
        clauses.add(groupBy);

        return this;
    }

    /**
     * Builds a SelectStatement from the fields list provided.
     * All fields must exist within the same table (determined by the first field with a table reference).
     * The table to select from will be taken from the the first row field.
     */
    public static SelectStatement of(SqlFieldMap<String, SqlField> fields)
    {
        SqlTable baseTable = null;
        for (SqlField field : fields.values())
        {
            if (field.getClass().isAssignableFrom(RowField.class))
            {
                baseTable = ((RowField)field).getTable();
            }
            if (baseTable!=null) break;
        }

        SelectStatement select = SelectStatement.select(fields);

        if (baseTable !=null)
        {
            baseTable.setAlias("b");
            select.from(baseTable);
        }
        return select;
    }

    /**
     * Builds a SelectStatement from a list of SelectProtos
     * The select statement will be built based on the order of the protos in the list.
     * All protos must have a valid join object so that they may be joined together
     * The first proto will be used as the left most table.
     *
     * @param protos a list of protos to use to build the select statement
     * @return A SelectStatement object
     */
    public static SelectStatement of(List<SelectProto> protos)
    {
        //Validate the parameters
        if (protos ==null || protos.isEmpty()) throw new IllegalArgumentException("No protos were specified");

        SqlJoinClause leftTableJoin  = protos.get(0).getJoin();
        if (leftTableJoin == null) throw new IllegalArgumentException("Cannot create SelectStatement. The join for the first proto must not be null");


        //Get the fields that will comprise the fields in the Select Statement
        FieldMapBuilder builder = SqlFieldMap.newBuilder();
        for (SelectProto proto : protos)
        {
            builder.add(proto.getFields());
        }

        //Start a select statement using the collected fields and the left table from the first proto
        SelectStatement select = SelectStatement.select(builder.build());
        SqlTable baseTable = leftTableJoin.getLeftField().getTable();
        baseTable.setAlias("b");
        select.from(baseTable);

        //Get the tables and joins
        for (SelectProto proto : protos)
        {
            SqlJoinClause join = proto.getJoin();
            if (join == null) throw new IllegalArgumentException("Cannot create SelectStatement. The join for each proto in the list must not be null");
            select.join(join);
        }

        return select;
    }

}
