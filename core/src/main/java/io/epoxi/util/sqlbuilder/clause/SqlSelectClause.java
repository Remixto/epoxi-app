package io.epoxi.util.sqlbuilder.clause;

import com.google.cloud.bigquery.StandardSQLTypeName;
import io.epoxi.util.sqlbuilder.types.RowField;
import io.epoxi.util.sqlbuilder.types.SqlField;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap;

import java.util.ArrayList;
import java.util.List;

public class SqlSelectClause implements SqlClause {

    final SqlFieldMap<String, SqlField> selectFields;

    public SqlSelectClause(SqlField... expressions)
    {
        SqlFieldMap<String, SqlField> map = new SqlFieldMap<>();

        for(SqlField f : expressions)
        {
            map.put(f.getName(), f);
        }

        selectFields = map;
    }

    @SuppressWarnings("unchecked")
    public SqlSelectClause(SqlFieldMap<String, ? extends SqlField> sqlFieldMap)
    {
        selectFields = (SqlFieldMap<String, SqlField>)sqlFieldMap;
    }

    public String getSql()
    {
        List<SqlField> fields = new ArrayList<>(this.selectFields.values());

        return "SELECT      " + getSql(fields);
    }

    private String getSql(List<? extends SqlField> fields)
    {
        List<String> strings = new ArrayList<>();

        for(SqlField field : fields)        {

            if (field.getType() != StandardSQLTypeName.STRUCT)
            {
                strings.add(String.format("      %s", field.getPrefixPathName()));
            }
            else
            {
                RowField rowField = (RowField)field;
                String subFieldSql = getSql(rowField.getSubfields());
                strings.add(String.format("      STRUCT(%n      %s%n            ) as `%s`", subFieldSql, field.getName()));
            }
        }

        return String.join(",\n      ", strings);
    }

	public SqlFieldMap<String, SqlField> getSelectFields() {
		return selectFields;
	}

}
