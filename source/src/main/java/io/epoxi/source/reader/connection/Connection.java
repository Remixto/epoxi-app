package io.epoxi.source.reader.connection;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import io.epoxi.source.reader.SourceReader;



public interface Connection {
    
    PCollection<TableRow> getReader(Pipeline p);
    SourceReader getReader();
    
}