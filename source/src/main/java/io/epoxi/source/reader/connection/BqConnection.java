package io.epoxi.source.reader.connection;

import com.google.api.services.bigquery.model.TableRow;


import io.epoxi.repository.model.Stream;
import io.epoxi.source.reader.SourceReader;
import io.epoxi.source.reader.TabularReader;
import io.epoxi.util.sqlbuilder.statement.SelectStatement;
import io.epoxi.util.sqlbuilder.types.SqlFieldMap;
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