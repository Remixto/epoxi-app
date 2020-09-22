package io.epoxi.util.sqlbuilder.clause;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import io.epoxi.util.sqlbuilder.types.RowField;
import io.epoxi.util.sqlbuilder.types.SqlField;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap;

import java.util.ArrayList;
import java.util.List;

public class SqlCreateTableClause implements SqlClause {

    final TableId tableId;
    final SqlFieldMap<String, ? extends SqlField> fieldMap;
    final Boolean allowReplace;

    public SqlCreateTableClause(TableId tableId, Boolean allowReplace, SqlField... expressions)
    {
        SqlFieldMap<String, SqlField> map = new SqlFieldMap<>();

        for(SqlField f : expressions)
        {
            map.put(f.getName(), f);
        }

        this.tableId = tableId;
        this.allowReplace = allowReplace;
        this.fieldMap = map;
    }

    public SqlCreateTableClause(TableId tableId, Boolean allowReplace, Iterable<? extends SqlField> fields)
    {
        SqlFieldMap<String, SqlField> map = new SqlFieldMap<>();

        for(SqlField f : fields)
        {
            map.put(f.getName(), f);
        }

        this.tableId = tableId;
        this.allowReplace = allowReplace;
        this.fieldMap = map;
    }

    public SqlCreateTableClause(TableId tableId, Boolean allowReplace, SqlFieldMap<String, ? extends SqlField> map)
    {
        this.tableId = tableId;
        this.allowReplace = allowReplace;
        this.fieldMap = map;
    }

    public String getSql()
    {
        List<SqlField> fields = new ArrayList<>(this.fieldMap.values());

        return String.format("CREATE%sTABLE     `%s.%s` (%n", orReplace(allowReplace), tableId.getDataset(), tableId.getTable()) +
            String.format("%s%n)", getSql(fields, "", false));

    }

    private String getSql(List<? extends SqlField> fields, String indent, Boolean isSubfields)
    {
        List<String> strings = new ArrayList<>();

        StringBuilder sb = new StringBuilder();

        for (SqlField field : fields)
        {
            if (field.getType() != StandardSQLTypeName.STRUCT)
            {
                if(Boolean.TRUE.equals(isSubfields))
                {
                    RowField rowField = (RowField)field;
                    strings.add(String.format("%s     %s %s", indent, rowField.getPath(false), rowField.getType()));
                }
                else
                {
                    strings.add(String.format("%s     %s %s", indent, field.getPath(), field.getType()));
                }

            }
            else
            {
                RowField rowField = (RowField)field;
                String subFieldSql = getSql(rowField.getSubfields(), "          ", true);
                strings.add(String.format("     %s STRUCT<%n%s%n           >", field.getPath(), subFieldSql));
            }
        }

        sb.append(String.join(",\n", strings));

        return sb.toString();
    }

    public TableId getTableId() {
        return tableId;
    }

    public String orReplace(Boolean allowReplace)
    {
        String orReplace = " ";
        if (Boolean.TRUE.equals(allowReplace))
        {
            orReplace = " OR REPLACE ";
        }

        return orReplace;
    }

}
