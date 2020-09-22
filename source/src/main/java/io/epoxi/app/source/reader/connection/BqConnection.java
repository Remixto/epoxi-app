package io.epoxi.app.source.reader.connection;

import com.google.api.services.bigquery.model.TableRow;


import io.epoxi.app.repository.model.Stream;
import io.epoxi.app.source.reader.SourceReader;
import io.epoxi.app.source.reader.TabularReader;
import io.epoxi.app.util.sqlbuilder.statement.SelectStatement;
import io.epoxi.app.util.sqlbuilder.types.SqlFieldMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;

public class BqConnection implements Connection {

    String projectId;
    String query;

    public PCollection<TableRow> getReader(Pipeline p)
    {
        return 
        p.apply(
            BigQueryIO.readTableRows()
                .fromQuery(query)
                .usingStandardSql()
                .withMethod(Method.DIRECT_READ));
    }

    public SourceReader getReader() {
        return new TabularReader(this);
    }

   

    public static BqConnection of(Stream stream)
    {        
        BqConnection connection = new BqConnection();
        connection.projectId = stream.getSource().getProject().getName();
        connection.query = SelectStatement.of(new SqlFieldMap.FieldMapBuilder().add(stream.getSqlFieldMap()).build()).getSql();

        return connection;
    }

    

}