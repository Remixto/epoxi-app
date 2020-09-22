package io.epoxi.util.sqlbuilder.clause;

import io.epoxi.util.sqlbuilder.types.SqlField;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class SqlGroupByClause implements SqlClause {

    final SqlFieldMap<String, ?> selectFields;

    public SqlGroupByClause(SqlField... expressions)
    {
        SqlFieldMap<String, SqlField> map = new SqlFieldMap<>();

        for(SqlField f : expressions)
        {
            map.put(f.getName(), f);
        }

        selectFields = map;
    }

    public SqlGroupByClause(SqlFieldMap<String, ?>  sqlFieldMap)
    {
        selectFields = sqlFieldMap;
    }

    public String getSql()
    {
        List<String> strings = new ArrayList<>();

        for(Entry<String, ?> entry : selectFields.entrySet())
        {
            SqlField field = (SqlField)entry.getValue();
            strings.add(String.format("      %s", field.getPrefixPathName()));
        }

        return "GROUP BY      " + String.join(",\n      ", strings);
    }





}
