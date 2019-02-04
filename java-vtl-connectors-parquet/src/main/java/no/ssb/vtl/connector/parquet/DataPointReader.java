package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.connector.parquet.converters.DataPointMaterializer;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class DataPointReader extends ReadSupport<DataPoint> {

    private final DataStructure structure;
    private Configuration configuration;
    private MessageType schema;

    private DataPointReader(DataStructure structure) {
        this.structure = structure;
    }

    public static Builder builder(InputFile file, DataStructure structure) {
        return new Builder(file, structure);
    }

    @Override
    public ReadContext init(InitContext context) {
        this.configuration = context.getConfiguration();
        this.schema = context.getFileSchema();
        return new ReadContext(this.schema, Collections.emptyMap());
    }

    @Override
    public RecordMaterializer<DataPoint> prepareForRead(Configuration configuration, Map map, MessageType messageType,
                                                        ReadContext readContext) {
        return new DataPointMaterializer(structure);
    }

    public static class Builder extends ParquetReader.Builder<DataPoint> {

        private final DataStructure structure;

        private Builder(InputFile file, DataStructure structure) {
            super(file);
            this.structure = Objects.requireNonNull(structure);
        }

        @Override
        protected ReadSupport<DataPoint> getReadSupport() {
            return new DataPointReader(structure);
        }
    }
}
