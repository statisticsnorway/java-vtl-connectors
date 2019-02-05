package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.Ordering;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ParquetDataset implements Dataset {

    private final InputFile file;
    private DataStructure structure;

    public ParquetDataset(InputFile file, DataStructure structure) {
        this.file = file;
        this.structure = structure;
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Ordering orders, Filtering filtering, Set<String> components) {

        // Convert the filter so Parquet can handle it.
        FilterPredicate predicate = new FilterConverter().apply(structure, filtering);
        // Get projection.
        Set<String> projectedColumns = !components.isEmpty() ? components : structure.keySet();


        try {
            ParquetReader.Builder<DataPoint> builder = DataPointReader.builder(file);
            // TODO: Need to work on the projection in VTL .withColumns(projectedColumns);
            if (predicate != null) {
                builder = builder.withFilter(FilterCompat.get(predicate));
            }

            ParquetReader<DataPoint> reader = builder.build();

            // TODO: Defer.
            Stream<DataPoint> stream = StreamSupport.stream(new Spliterators.AbstractSpliterator<DataPoint>(Long.MAX_VALUE, Spliterator.IMMUTABLE) {
                @Override
                public boolean tryAdvance(Consumer<? super DataPoint> action) {
                    try {
                        DataPoint read = reader.read();
                        if (read != null) {
                            action.accept(read);
                            return true;
                        } else {
                            return false;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, false);

            // TODO: Avoid re-ordering if compatible.
            return Optional.of(stream.sorted(orders).onClose(() -> {
                try {
                    reader.close();
                } catch (IOException e) {
                    // Ignore
                }
            }));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        return structure;
    }
}
