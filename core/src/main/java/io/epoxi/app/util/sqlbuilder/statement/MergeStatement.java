package io.epoxi.app.util.sqlbuilder.statement;

import io.epoxi.app.util.sqlbuilder.clause.*;
import io.epoxi.app.util.sqlbuilder.types.*;
import io.epoxi.util.sqlbuilder.clause.*;
import io.epoxi.util.sqlbuilder.types.*;

import java.util.List;


public class MergeStatement extends SqlStatement {

    private SqlTable targetTable;
    private SqlFieldMap<String, SqlField> sourceFieldMap;

    public static MergeStatement merge(SqlTable table)
    {
        MergeStatement statement = new MergeStatement();
        statement.targetTable = table;

        SqlMergeClause select = new SqlMergeClause(statement, table);
        statement.clauses.add(select);

        return statement;
    }

    public MergeStatement using(SqlTable table, SqlPredicate predicate)
    {
        using(table, null, predicate);

        return this;
    }

    /**
     * Specifies the source fields for the Merge Statement
     * @param table The table that will be merged into
     * @param extraFields Extra fields to add to the source
     * @param predicate The predicate between the source and target tables
     * @return A merge statement object
     */
    public MergeStatement using(SqlTable table, SqlFieldMap<String, SqlField> extraFields, SqlPredicate predicate)
    {
        //Build a total set of fields that will comprise the source, by union-ing table and extraFields together
        SqlFieldMap<String, SqlField> usingFields = new SqlFieldMap<>();
        usingFields.putAll(table.getFields());

        if (extraFields != null)
        {
            usingFields = SqlFieldMap.newBuilder().add(usingFields).add(extraFields).build();
        }

        sourceFieldMap = usingFields;

        //Form a sql statement
        SelectStatement selectStatement = SelectStatement.select(usingFields).from(table);

        using(selectStatement, table.getAlias(), predicate);

        return this;
    }

    private MergeStatement using(SelectStatement selectStatement, String statementAlias, SqlPredicate predicate)
    {
        SqlUsingClause clause = new SqlUsingClause(selectStatement, statementAlias, predicate);
        clauses.add(clause);
        return this;
    }

    public MergeStatement whenMatched()
    {
        //Get a copy of the source fields and remove the DateCreated so that it won't be matched
        //(we don't update this field in the WHEN MATCHED clause)
        SqlFieldMap<String, SqlField> sourceFieldMapCopy = new SqlFieldMap<>();
        sourceFieldMapCopy.putAll(getSourceFieldMap());
        sourceFieldMapCopy.remove("Date_Created");

        List<SqlFieldPair> pairs = SqlFieldPairList.of(getTargetTable(), sourceFieldMapCopy);

        SqlUpdateSetClause update = new SqlUpdateSetClause(pairs);
        SqlWhenMatchedClause whenMatched = new SqlWhenMatchedClause(update);
        clauses.add(whenMatched);

        return this;
    }

    public MergeStatement whenNotMatchedByTarget()
    {
        //Get the fields that match between source and target
        SqlFieldPairList pairs = SqlFieldPairList.of(getTargetTable(), getSourceFieldMap());


        SqlFieldMap<String, SqlField> fields = pairs.getField1s();

        InsertStatement insertStatement = InsertStatement.insert(fields).values(fields);

        SqlWhenNotMatchedByTargetClause whenNotMatched = new SqlWhenNotMatchedByTargetClause(insertStatement);
        clauses.add(whenNotMatched);

        return this;
    }

    public MergeStatement whenNotMatchedBySource(SqlFieldPair... pairs)
    {
        SqlUpdateSetClause update = new SqlUpdateSetClause(pairs);
        SqlWhenNotMatchedBySourceClause whenNotMatched = new SqlWhenNotMatchedBySourceClause(update);
        clauses.add(whenNotMatched);

        return this;
    }

    public MergeStatement whenNotMatchedBySource(List<SqlFieldPair> pairs)
    {
        SqlFieldPair[] pairArray = pairs.toArray(new SqlFieldPair[pairs.size()]);
        whenNotMatchedBySource(pairArray);

        return this;
    }

    public MergeStatement update(SqlFieldPair... expressions)
    {
        SqlUpdateSetClause select = new SqlUpdateSetClause(expressions);
        clauses.add(select);

        return this;
    }


    public SqlFieldMap<String, SqlField> getSourceFieldMap()
    {
        return sourceFieldMap;
    }

    public SqlTable getTargetTable()
    {
        return targetTable;
    }
}
