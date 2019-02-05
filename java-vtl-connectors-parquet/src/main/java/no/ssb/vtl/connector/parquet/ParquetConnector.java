package no.ssb.vtl.connector.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.connectors.utils.jackson.DataStructureDeserializer;
import no.ssb.vtl.connectors.utils.jackson.DataStructureSerializer;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

public abstract class ParquetConnector implements Connector {

    public static final String STRUCTURE_META_NAME = "vtl.structure";
    public static final String DATAPOINT_TYPE_NAME = "no.ssb.vtl.datapoint";
    public static final ObjectMapper MAPPER;
    public static final int PAGE_SIZE = 1048576;
    public static final int ROW_GROUP_SIZE = 5242880;
    public static final boolean ENABLE_DICTIONARY = true;
    public static final CompressionCodecName COMPRESSION_CODEC = CompressionCodecName.SNAPPY;

    static {
        MAPPER = new ObjectMapper();
        SimpleModule module = new SimpleModule("vtl data structure");
        module.addDeserializer(DataStructure.class, new DataStructureDeserializer());
        module.addSerializer(DataStructure.class, new DataStructureSerializer());
        MAPPER.registerModule(module);
    }

    @Override
    public boolean canHandle(String identifier) {
        return true;
    }

    abstract InputFile getFile(String identifier) throws ConnectorException;

    abstract OutputFile putFile(String identifier) throws ConnectorException;

    @Override
    public final Dataset getDataset(String identifier) throws ConnectorException {
        InputFile file = getFile(identifier);
        try (ParquetFileReader open = ParquetFileReader.open(file)) {
            Map<String, String> metadata = open.getFileMetaData().getKeyValueMetaData();
            String jsonDataStructure = metadata.get(STRUCTURE_META_NAME);
            try {
                DataStructure structure = MAPPER.readValue(jsonDataStructure, DataStructure.class);
                return new ParquetDataset(file, structure);
            } catch (IOException e) {
                throw new IllegalArgumentException("could not parse data structure");
            }
        } catch (IOException e) {
            throw new ConnectorException("could not read file" + file);
        }
    }


    @Override
    public final Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException {
        OutputFile outputFile = putFile(identifier);
        // TODO: Make configurable.
        DataPointWriter.Builder builder = DataPointWriter.builder(outputFile, dataset.getDataStructure())
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withCompressionCodec(COMPRESSION_CODEC)
                .withPageSize(PAGE_SIZE)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withDictionaryEncoding(ENABLE_DICTIONARY);
        try (Stream<DataPoint> data = dataset.getData(); ParquetWriter<DataPoint> writer = builder.build()) {
            data.forEach(object -> {
                try {
                    writer.write(object);
                } catch (IOException e) {
                    throw new RuntimeException(String.format("could not write data point %s to %s", object,
                            identifier));
                }
            });
        } catch (IOException e) {
            throw new ConnectorException(String.format("could not write dataset %s to %s", dataset, identifier));
        }
        return getDataset(identifier);
    }
}
