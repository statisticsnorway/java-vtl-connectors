package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.connector.parquet.io.LocalOutputFile;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;

import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;

public class DataPointWriterTest {


    @Test
    public void testWrite() throws IOException {
        DataStructure structure = DataStructure.of(
                "id", IDENTIFIER, String.class,
                "me", MEASURE, String.class,
                "at", MEASURE, String.class
        );

        java.nio.file.Path path = FileSystems.getDefault().getPath("test.parquet");
        org.apache.parquet.io.OutputFile outputFile = LocalOutputFile.nioPathToOutputFile(path);
        ParquetWriter<DataPoint> writer = DataPointWriter.builder(outputFile, structure)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withDictionaryEncoding(true)
                .build();
        try {
            for (int i = 0; i < 1_000_000; i++) {
                writer.write(DataPoint.create("id" + i, "me" + 1, "at" + 1));
            }
        } finally {
            writer.close();
        }

    }
}