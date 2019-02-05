package no.ssb.vtl.connector.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import no.ssb.vtl.connector.parquet.converters.DataPointMaterializer;
import no.ssb.vtl.connectors.spring.converters.DataStructureHttpConverter;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DataPointReader extends ReadSupport<DataPoint> {

    private final DataStructure structure;
    private final Set<String> columns;

    private MessageType schema;

    // Called in Builder#getReadSupport().
    private DataPointReader(DataStructure structure, Set<String> columns) {
        this.structure = structure;
        this.columns = columns;
    }

    public static Builder builder(InputFile file) {
        return new Builder(file);
    }

    @Override
    public ReadContext init(InitContext context) {

        // TODO: Extract DataStructure here.
        // context.getKeyValueMetadata()

        // Compute a projected data structure.
        DataStructure subStructure = projectStructure();
        MessageType subSchema = new DataStructureConverter().apply(subStructure);
        this.schema = ReadSupport.getSchemaForRead(context.getFileSchema(), subSchema);

        return new ReadContext(this.schema, Collections.emptyMap());
    }

    private DataStructure projectStructure() {
        return DataStructure.copyOf(Maps.filterKeys(structure, columns::contains)).build();
    }

    @Override
    public RecordMaterializer<DataPoint> prepareForRead(Configuration configuration, Map map, MessageType messageType,
                                                        ReadContext readContext) {
        return new DataPointMaterializer(projectStructure());
    }

    public static class Builder extends ParquetReader.Builder<DataPoint> {

        private final InputFile file;
        private DataStructure structure;
        private Set<String> columns;

        private Builder(InputFile file) {
            super(file);
            this.file = Objects.requireNonNull(file);
        }

        /**
         * Set of columns to read.
         */
        public Builder withColumns(Set<String> columns) {
            this.columns = columns;
            return this;
        }

        @Override
        public ParquetReader<DataPoint> build() throws IOException {
            // Read the structure from the file.
            try (ParquetFileReader parquetFile = ParquetFileReader.open(this.file)) {
                Map<String, String> metaData = parquetFile.getFileMetaData().getKeyValueMetaData();
                String structureJson = metaData.get(ParquetConnector.STRUCTURE_META_NAME);
                // TODO: Extract jackson ser-deser from spring converter to utils.
                DataStructureHttpConverter converter = new DataStructureHttpConverter(new ObjectMapper());
                this.structure = converter.read(DataStructure.class, new HttpInputMessage() {
                    @Override
                    public InputStream getBody() {
                        return new ByteArrayInputStream(structureJson.getBytes());
                    }

                    @Override
                    public HttpHeaders getHeaders() {
                        return new HttpHeaders();
                    }
                });
                if (columns == null) {
                    this.columns = this.structure.keySet();
                }
            }
            return super.build();
        }

        @Override
        protected ReadSupport<DataPoint> getReadSupport() {
            return new DataPointReader(structure, columns);
        }
    }
}
