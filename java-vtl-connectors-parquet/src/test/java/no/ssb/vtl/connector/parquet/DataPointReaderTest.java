package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static no.ssb.vtl.connector.parquet.io.LocalInputFile.nioPathToInputFile;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;

public class DataPointReaderTest {

    @Test
    public void testRead() throws IOException {

        DataStructure structure = DataStructure.of(
                "id", IDENTIFIER, String.class,
                "me", MEASURE, String.class,
                "at", MEASURE, String.class
        );

        Operators.BinaryColumn id = FilterApi.binaryColumn("id");
        FilterPredicate predicate = FilterApi.and(FilterApi.gt(id, Binary.fromCharSequence("id100000")), FilterApi.lt(id, Binary.fromCharSequence("id200000")));

        Path path = FileSystems.getDefault().getPath("test.parquet");
        org.apache.parquet.io.InputFile inputFile = nioPathToInputFile(path);
        ParquetReader<DataPoint> reader = DataPointReader.builder(inputFile, structure)
                .withFilter(FilterCompat.get(predicate))
                .build();
        try {
            DataPoint point;
            while ((point = reader.read()) != null) {
                System.out.println(point);
            }
        } finally {
            reader.close();
        }

    }
}