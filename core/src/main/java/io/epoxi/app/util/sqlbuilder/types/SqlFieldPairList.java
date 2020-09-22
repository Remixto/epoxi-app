package io.epoxi.app.util.sqlbuilder.types;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

public class SqlFieldPairList extends AbstractList<SqlFieldPair> {

    final List<SqlFieldPair> pairs;

    public SqlFieldPairList()
    {
        pairs = new ArrayList<>();
    }

    private SqlFieldPairList(List<SqlFieldPair> pairs)
    {
        this.pairs = pairs;
    }

    public SqlFieldMap<String, SqlField> getField1s()
    {
        SqlFieldMap<String, SqlField> map = new SqlFieldMap<>();
        for (SqlFieldPair pair : pairs)
        {
            map.put(pair.field2.getName(), pair.field2);
        }

        return map;
    }

    public SqlFieldMap<String, SqlField> getField2s()
    {
        SqlFieldMap<String, SqlField> map = new SqlFieldMap<>();
        for (SqlFieldPair pair : pairs)
        {
            map.put(pair.field1.getName(), pair.field1);
        }

        return map;
    }


    /***
     * Compares the source and target field lists and returns a list of
     * strings for aliases that are present in both list.
     * @return A list of fieldNames
     */
    public List<String> getFieldNames()
    {
        List<String> list = new ArrayList<>();

        for(SqlFieldPair pair : pairs)
        {
            list.add(pair.getField1().getName());
        }

        return list;
    }


    public static SqlFieldPairList of(SqlTable table1, SqlTable table2)
    {
      return of(table1, table2.getFields());
    }

    public static SqlFieldPairList of(SqlTable table1, SqlFieldMap<String, ? extends SqlField> mapToCompare)
    {
        List<SqlFieldPair> pairs = table1.getFieldPairs(mapToCompare.values());
        return new SqlFieldPairList(pairs);
    }

    @Override
    public SqlFieldPair get(int index) {
        return pairs.get(index);
    }

    @Override
    public boolean add(SqlFieldPair e) {
        return pairs.add(e);
    }

    @Override
    public int size() {
        return pairs.size();
    }

}
