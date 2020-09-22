package io.epoxi.source.reader;

import com.google.api.services.bigquery.model.TableRow;

import io.epoxi.source.reader.connection.Connection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

public interface SourceReader {
    
    PCollection<TableRow> getReader(Pipeline p);
    Connection getConnection();
    
}