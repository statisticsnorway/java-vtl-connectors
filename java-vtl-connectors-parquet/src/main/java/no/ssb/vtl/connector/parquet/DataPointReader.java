package no.ssb.vtl.connector.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Maps;
import no.ssb.vtl.connector.parquet.converters.DataPointMaterializer;
import no.ssb.vtl.connectors.utils.jackson.DataStructureDeserializer;
import no.ssb.vtl.connectors.utils.jackson.DataStructureSerializer;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static no.ssb.vtl.connector.parquet.ParquetConnector.MAPPER;
import static no.ssb.vtl.connector.parquet.ParquetConnector.STRUCTURE_META_NAME;

public class DataPointReader extends ReadSupport<DataPoint> {

    private final DataStructureConverter dataStructureConverter = new DataStructureConverter();
    private final DataStructureDeserializer structureDeserializer = new DataStructureDeserializer();
    private Set<String> columns;
    private DataStructure structure;

    // Called in Builder#getReadSupport().
    private DataPointReader(Set<String> columns) {
        this.columns = columns;
    }

    public static Builder builder(InputFile file) {
        return new Builder(file);
    }

    @Override
    public ReadContext init(InitContext context) {

        this.structure = parseDataStructure(context);
        if (this.columns == null) {
            this.columns = this.structure.keySet();
        }

        // Compute a projected data structure.
        DataStructure projectedStructure = projectStructure();
        MessageType subSchema = dataStructureConverter.apply(projectedStructure);
        MessageType schema = ReadSupport.getSchemaForRead(context.getFileSchema(), subSchema);

        return new ReadContext(schema, Collections.emptyMap());
    }

    private DataStructure parseDataStructure(InitContext context) {
        Set<String> strings = context.getKeyValueMetadata().get(STRUCTURE_META_NAME);
        if (strings.isEmpty() || strings.size() > 1) {
            throw new IllegalArgumentException("got mor than one structure in metadata");
        }
        String jsonDataStructure = strings.iterator().next();
        try {
            return MAPPER.readValue(jsonDataStructure, DataStructure.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("could not parse data structure");
        }
    }

    /**
     * Create a projection of the data structure.
     */
    private DataStructure projectStructure() {
        return DataStructure.copyOf(Maps.filterKeys(structure, columns::contains)).build();
    }

    @Override
    public RecordMaterializer<DataPoint> prepareForRead(Configuration configuration, Map map, MessageType messageType,
                                                        ReadContext readContext) {
        return new DataPointMaterializer(projectStructure());
    }

    public static class Builder extends ParquetReader.Builder<DataPoint> {

        private Set<String> columns;

        private Builder(InputFile file) {
            super(file);
        }

        /**
         * Set of columns to read.
         */
        public Builder withColumns(Set<String> columns) {
            this.columns = columns;
            return this;
        }

        //@Override
        //public ParquetReader<DataPoint> build() throws IOException {
        //    // Read the structure from the file.
        //    try (ParquetFileReader parquetFile = ParquetFileReader.open(this.file)) {
        //        Map<String, String> metaData = parquetFile.getFileMetaData().getKeyValueMetaData();
        //        String structureJson = metaData.get(STRUCTURE_META_NAME);
        //        // TODO: Extract jackson ser-deser from spring converter to utils.
        //        DataStructureHttpConverter converter = new DataStructureHttpConverter(new ObjectMapper());
        //        this.structure = converter.read(DataStructure.class, new HttpInputMessage() {
        //            @Override
        //            public InputStream getBody() {
        //                return new ByteArrayInputStream(structureJson.getBytes());
        //            }
//
        //            @Override
        //            public HttpHeaders getHeaders() {
        //                return new HttpHeaders();
        //            }
        //        });
        //        if (columns == null) {
        //            this.columns = this.structure.keySet();
        //        }
        //    }
        //    return super.build();
        //}

        @Override
        protected ReadSupport<DataPoint> getReadSupport() {
            return new DataPointReader(columns);
        }
    }
}
