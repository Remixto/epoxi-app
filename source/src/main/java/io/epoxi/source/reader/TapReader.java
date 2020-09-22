package io.epoxi.source.reader;

import com.google.api.services.bigquery.model.JsonObject;
import com.google.api.services.bigquery.model.TableRow;
import io.epoxi.source.reader.connection.Connection;
import io.epoxi.repository.model.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.talend.sdk.component.singer.java.Singer;
import org.talend.sdk.component.singer.java.SingerArgs;
import org.talend.sdk.component.singer.kitap.Kitap;

import java.util.ArrayList;
import java.util.List;

public class TapReader implements SourceReader {

    PCollection<String> textData;
    private final SingerArgs singerArgs;
    private Connection connection;

    public TapReader(Stream stream) {
        this.singerArgs = configure(stream);
    }

    private SingerArgs configure(Stream stream) {
        JsonObject config = null;
        JsonObject state = null;
        JsonObject catalog = null;

        List<String> args = new ArrayList<>();
        args.add(String.format("--%s %s", "config", config.toString()));

        SingerArgs singerArgs = new SingerArgs(args.toArray(new String[args.size()]));

        return singerArgs;
    }

    public void read() {
        Singer singer = new Singer();
        Kitap kitap = new Kitap(singerArgs, singer);
        kitap.run();
    }

    public PCollection<TableRow> getReader(Pipeline p) {

        read();

        // Convert stdOutput to table Rows //TODO

        return null;
    }    

    public Connection getConnection() {
        return connection;
    }
}