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

import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;

public class ParquetDataset implements Dataset {

    private final InputFile file;
    private DataStructure structure;

    public ParquetDataset(InputFile file) {
        this.file = file;
        // TODO: Ser deser.
        this.structure = DataStructure.builder()
                .put("AARGANG", IDENTIFIER, String.class)
                .put("ART_SEKTOR", IDENTIFIER, String.class)
                .put("BYDEL", IDENTIFIER, String.class)
                .put("FUNKSJON_KAPITTEL", IDENTIFIER, String.class)
                .put("KONTOKLASSE", IDENTIFIER, String.class)
                .put("PERIODE", IDENTIFIER, String.class)
                .put("REGION", IDENTIFIER, String.class)
                .put("BELOP", MEASURE, Long.class)
                .put("BRUTTO_DRIFT_INNT", MEASURE, Long.class)
                .put("BRUTTO_DRIFT_UTG", MEASURE, Long.class)
                .put("BRUTTO_DRIFTSRES", MEASURE, Long.class)
                .put("BRUTTO_DRUTG_TJE", MEASURE, Long.class)
                .put("BRUTTO_INVEST", MEASURE, Long.class)
                .put("INNTEKT_DRIFT", MEASURE, Long.class)
                .put("INNTEKT_INVEST", MEASURE, Long.class)
                .put("KORR_BR_DRUTG_TJ", MEASURE, Long.class)
                .put("LINJENUMMER", MEASURE, Long.class)
                .put("LONN", MEASURE, Long.class)
                .put("MOTP_AVSKR", MEASURE, Long.class)
                .put("NETTO_DRIFTSRES", MEASURE, Long.class)
                .put("NETTO_INVEST", MEASURE, Long.class)
                .put("RAMMETILSKUDD", MEASURE, Long.class)
                .put("RES_EKST_FINTRANS", MEASURE, Long.class)
                .put("SKATT", MEASURE, Long.class)
                .put("SUM_INNTEKT_DRIFT", MEASURE, Long.class)
                .put("SUM_INNTEKT_INVEST", MEASURE, Long.class)
                .put("SUM_UTGIFT_DRIFT", MEASURE, Long.class)
                .put("SUM_UTGIFT_INVEST", MEASURE, Long.class)
                .put("TILSKUDD_MV", MEASURE, Long.class)
                .put("UTGIFT_DRIFT", MEASURE, Long.class)
                .put("UTGIFT_INVEST", ATTRIBUTE, String.class)
                .put("AVGIVER_ID", ATTRIBUTE, Long.class)
                .put("DELREG_NR", ATTRIBUTE, String.class)
                .put("ENHETS_ID", ATTRIBUTE, String.class)
                .put("ENHETS_TYPE", ATTRIBUTE, String.class)
                .put("FORETAKSNR", ATTRIBUTE, Long.class)
                .put("ID", ATTRIBUTE, String.class)
                .put("KOMMUNENAVN", ATTRIBUTE, String.class)
                .put("KVARTAL", ATTRIBUTE, String.class)
                .put("LEVERANDOR", ATTRIBUTE, String.class)
                .put("LOPENR", ATTRIBUTE, String.class)
                .put("ORG_NR", ATTRIBUTE, Long.class)
                .put("RAD_NR", ATTRIBUTE, String.class)
                .put("SKJEMA", ATTRIBUTE, String.class)
                .put("STATUS", ATTRIBUTE, String.class)
                .put("TJENESTEGRUPPE", ATTRIBUTE, String.class)
                .build();
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
