package io.epoxi.app.source.reader;

import com.google.api.services.bigquery.model.TableRow;

import io.epoxi.app.source.reader.connection.Connection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

public class TabularReader implements SourceReader {

    private final Connection connection;

    public TabularReader(Connection connection) {
        this.connection = connection;
    }

    public PCollection<TableRow> getReader(Pipeline p) {

        // Build pipeline from query
        return connection.getReader(p);
    }

    public Connection getConnection() {
        return connection;
    }

    
    
}