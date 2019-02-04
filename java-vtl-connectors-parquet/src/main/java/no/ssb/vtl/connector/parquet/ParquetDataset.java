package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.Ordering;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class ParquetDataset implements Dataset {

    private final InputFile file;

    public ParquetDataset(InputFile file) {
        this.file = file;
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Ordering orders, Filtering filtering, Set<String> components) {
        return Optional.empty();

        //try {
        //    Util.readPageHeader(file.newStream()).
        //} catch (IOException e) {
        //    e.printStackTrace();
        //}
//
        //DataPointReader.builder(file, )
        //return Optional.empty();
    }

    @Override
    public Stream<DataPoint> getData() {
        return null;
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getSize() {
        return Optional.empty();
    }

    @Override
    public DataStructure getDataStructure() {
        return null;
    }
}
