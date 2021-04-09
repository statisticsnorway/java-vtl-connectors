package no.ssb.vtl.connector.parquet;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import no.ssb.vtl.connector.parquet.io.LocalOutputFile;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;

public class DataPointWriterTest {


    @Test
    public void testWrite() throws IOException {

        DataStructure dataStructure = DataStructure.builder()
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

        String name = "8986396a-be56-434e-8491-fd1148d8c2b9";
        //String name = "ba020a95-7810-4ea6-a2e8-08ac56f7e344";

        Path file = FileSystems.getDefault().getPath(name + ".csv");
        BufferedReader reader = Files.newBufferedReader(file);
        CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
        CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).withSkipLines(1).build();

        java.nio.file.Path path = FileSystems.getDefault().getPath(name + ".parquet");
        org.apache.parquet.io.OutputFile outputFile = LocalOutputFile.nioPathToOutputFile(path);
        ParquetWriter<DataPoint> writer = DataPointWriter.builder(outputFile, dataStructure)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(1048576)
                .withRowGroupSize(5242880)
                .withDictionaryEncoding(true)
                .build();

        try {
            Class[] types = dataStructure.values().stream().map(Component::getType).toArray(Class[]::new);
            String[] column = dataStructure.keySet().stream().toArray(String[]::new);
            Object[] buffer = new Object[dataStructure.size()];
            for (String[] values : csvReader) {
                for (int i = 0; i < values.length; i++) {
                    try {
                        if ("".equals(values[i])) {
                            buffer[i] = null;
                        } else {
                            if (types[i].equals(Long.class)) {
                                buffer[i] = Long.parseLong(values[i]);
                            } else if (types[i].equals(String.class)) {
                                buffer[i] = values[i];
                            } else {
                                throw new IllegalArgumentException("unknow type");
                            }
                        }
                    } catch (Exception e) {
                        throw new IllegalArgumentException("could not convert " + values[i] + "(" + column[i] + ") to " + types[i]);
                    }
                }
                writer.write(DataPoint.create(buffer));
            }
        } finally {
            csvReader.close();
            writer.close();
        }


    }
}