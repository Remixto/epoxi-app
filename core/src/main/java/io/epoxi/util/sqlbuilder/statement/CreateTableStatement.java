package io.epoxi.util.sqlbuilder.statement;

import com.google.cloud.bigquery.TableId;
import io.epoxi.util.sqlbuilder.clause.SqlClause;
import io.epoxi.util.sqlbuilder.clause.SqlCreateTableClause;
import io.epoxi.util.sqlbuilder.types.SqlField;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap;

import java.util.List;

public class CreateTableStatement  extends SqlStatement implements SqlClause{

    private TableId tableId;

    public static CreateTableStatement createTable(TableId tableId, Boolean allowReplace, SqlField... expressions)
    {
        SqlFieldMap<String, ? extends SqlField> map = SqlFieldMap.of(expressions);
        return createTable(tableId, allowReplace, map);
    }

    public static CreateTableStatement createTable(TableId tableId, Boolean allowReplace, List<? extends SqlField> fields)
    {
        SqlFieldMap<String, ? extends SqlField> map = SqlFieldMap.of(fields);
        return createTable(tableId, allowReplace, map);
    }

    public static CreateTableStatement createTable(TableId tableId, Boolean allowReplace, SqlFieldMap<String, ? extends SqlField> sqlFieldMap)
    {
        CreateTableStatement statement = new CreateTableStatement();
        SqlCreateTableClause createTable = new SqlCreateTableClause(tableId, allowReplace, sqlFieldMap);
        statement.clauses.add(createTable);

        return statement;
    }

    public TableId getTableId() {
        return tableId;
    }




}
