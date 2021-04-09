package no.ssb.vtl.connector.parquet;

import com.google.common.collect.ImmutableSet;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.VtlFiltering;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static no.ssb.vtl.connector.parquet.io.LocalInputFile.nioPathToInputFile;
import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
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
        InputFile inputFile = nioPathToInputFile(path);
        ParquetReader<DataPoint> reader = DataPointReader.builder(inputFile)
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

    @Test
    public void testReadColumns() throws IOException {

        DataStructure structure = DataStructure.builder()
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

        Path path = FileSystems.getDefault().getPath("8986396a-be56-434e-8491-fd1148d8c2b9.parquet");
        InputFile inputFile = nioPathToInputFile(path);

        Configuration conf = new Configuration();

        // Not available as API. @see GroupReadSupport
        //conf.set(ReadSupport.PARQUET_READ_SCHEMA, "test");

        VtlFiltering.Builder vtlFiltering = VtlFiltering.using(structure).and(
                VtlFiltering.gt("LONN", 0),
                VtlFiltering.lt("ART_SEKTOR", "700"),
                VtlFiltering.gt("ART_SEKTOR", "130")
        );

        FilterPredicate predicate = new FilterConverter().apply(structure, vtlFiltering.build());

        ParquetReader<DataPoint> reader = DataPointReader.builder(inputFile)
                .withColumns(ImmutableSet.of("ART_SEKTOR", "LONN"))
                .withFilter(FilterCompat.get(predicate))
                .withConf(conf)
                .build();


        List<DataPoint> result = new ArrayList<>(1024 * 256);
        try {
            DataPoint point;
            while ((point = reader.read()) != null) {
                result.add(point);
            }
        } finally {
            reader.close();
        }

        System.out.println(result.size());


    }
}