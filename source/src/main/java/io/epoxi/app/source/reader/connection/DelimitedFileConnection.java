package io.epoxi.app.source.reader.connection;

import com.google.api.services.bigquery.model.TableRow;
import io.epoxi.app.source.reader.SourceReader;
import io.epoxi.app.source.reader.TabularReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

public class DelimitedFileConnection implements Connection {

    public PCollection<TableRow> getReader(Pipeline p) {
        // String name = "name of the transformation";

        // Options options =  p.getOptions().as(Options.class);
        // String loadingBucket = options.getLoadingBucketURL();
        // String objectToLoad = storedObjectName(loadingBucket, name);
        // p.apply(name, TextIO.read().from(objectToLoad));

        // PCollection<TableRow> tableRows = p.apply(); // transform text file to PCollection<TableRow>
        // return tableRows;

       return null;
    }

    public SourceReader getReader() {
        return new TabularReader(this);
    }
    

}