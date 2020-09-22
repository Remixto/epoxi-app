package io.epoxi.app.util.sqlbuilder.statement;

import com.google.cloud.bigquery.TableResult;
import io.epoxi.app.cloud.bq.BqService;
import io.epoxi.app.cloud.logging.StatusCode;
import io.epoxi.app.util.sqlbuilder.SqlBuilderException;
import io.epoxi.app.util.sqlbuilder.clause.SqlClause;
import io.epoxi.app.util.sqlbuilder.types.SqlTable;

import java.util.ArrayList;
import java.util.List;

public abstract class SqlStatement {

    protected final List<SqlClause> clauses = new ArrayList<>();
    protected final List<SqlTable> tables = new ArrayList<>();

    public List<SqlTable> getTables() {
        return tables;
    }

    public String getSql()
    {
       return getSql(true);
    }

    public String getSql(Boolean addTerminator)
    {
        List<String> strings = new ArrayList<>();
        SqlClause currentClause = null;

        try{
            for(SqlClause clause : clauses)
            {
                currentClause = clause;
                strings.add(clause.getSql());
            }

            String sql = String.format("%s", String.join("\n", strings)) ;
            if (Boolean.TRUE.equals(addTerminator)) sql = sql + ";";
            return sql;
        }
        catch(Exception ex )
        {
            String msg = "Error getting SQL for clause";
            if (currentClause !=null) msg = String.format(" '%s'", currentClause.getClass());
            throw new SqlBuilderException(msg, ex, StatusCode.INTERNAL);
        }
    }

    public TableResult render(BqService bqService)
    {
        String sql = getSql();
        return bqService.getQueryResult(sql);
    }

    public void validate(BqService bqService)
    {
        String sql = getSql();
        bqService.validateQuery(sql);
    }


}
