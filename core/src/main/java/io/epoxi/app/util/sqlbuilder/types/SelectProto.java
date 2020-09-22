package io.epoxi.app.util.sqlbuilder.types;

import io.epoxi.app.util.sqlbuilder.clause.SqlJoinClause;

/**
 * Information that can contribute to a select statement, specifically a group of fields and join information
 * that provides join syntax to connect the fields to the main select statement.
 */
public class SelectProto {

    final SqlFieldMap<String, ? extends SqlField> fields;
    final SqlJoinClause join;

    private SelectProto(SqlFieldMap<String, ? extends SqlField> fields, SqlJoinClause join)
    {
        this.fields = fields;
        this.join = join;
    }

    public SqlFieldMap<String, ? extends SqlField> getFields() {
        return fields;
    }

    public SqlJoinClause getJoin() {
        return join;
    }

    /**
     * Creates a new SelectProto.
     * @param fields A list of fields that will be returned into the select.
     * All fields in the list that specify a table must specify the same table
     * that is specified in the join clause.
     * @param join  The key of the field list produced by dataTableId.
     * The fields participating in the join do not have to exist in the fields object.
     * However the fields must exist in the table object specified in the join fields.
     * @return a new SelectProto object
     */
    public static SelectProto of(SqlFieldMap<String, ? extends SqlField> fields, SqlJoinClause join)
    {
        return new SelectProto(fields, join);
    }
}
