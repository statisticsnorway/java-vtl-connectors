package no.ssb.vtl.connectors.utils.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.vtl.model.DataStructure;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;

import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DataStructureSerializerTest {

    public static final DataStructure STRUCTURE = DataStructure.builder()
            .put("idString", IDENTIFIER, String.class)
            .put("idLong", IDENTIFIER, Long.class)
            .put("idInteger", IDENTIFIER, Integer.class)
            .put("idFloat", IDENTIFIER, Float.class)
            .put("idDouble", IDENTIFIER, Double.class)
            .put("idBoolean", IDENTIFIER, Boolean.class)
            .put("idInstant", IDENTIFIER, Instant.class)

            .put("measureString", MEASURE, String.class)
            .put("measureLong", MEASURE, Long.class)
            .put("measureInteger", MEASURE, Integer.class)
            .put("measureFloat", MEASURE, Float.class)
            .put("measureDouble", MEASURE, Double.class)
            .put("measureBoolean", MEASURE, Boolean.class)
            .put("measureInstant", MEASURE, Instant.class)

            .put("attributeString", ATTRIBUTE, String.class)
            .put("attributeLong", ATTRIBUTE, Long.class)
            .put("attributeInteger", ATTRIBUTE, Integer.class)
            .put("attributeFloat", ATTRIBUTE, Float.class)
            .put("attributeDouble", ATTRIBUTE, Double.class)
            .put("attributeBoolean", ATTRIBUTE, Boolean.class)
            .put("attributeInstant", ATTRIBUTE, Instant.class)

            .build();

    public static final DataStructure STRUCTURE_NORMALIZED = DataStructure.builder()
            .put("idString", IDENTIFIER, String.class)
            .put("idLong", IDENTIFIER, Long.class)
            .put("idInteger", IDENTIFIER, Long.class)
            .put("idFloat", IDENTIFIER, Double.class)
            .put("idDouble", IDENTIFIER, Double.class)
            .put("idBoolean", IDENTIFIER, Boolean.class)
            .put("idInstant", IDENTIFIER, Instant.class)

            .put("measureString", MEASURE, String.class)
            .put("measureLong", MEASURE, Long.class)
            .put("measureInteger", MEASURE, Long.class)
            .put("measureFloat", MEASURE, Double.class)
            .put("measureDouble", MEASURE, Double.class)
            .put("measureBoolean", MEASURE, Boolean.class)
            .put("measureInstant", MEASURE, Instant.class)

            .put("attributeString", ATTRIBUTE, String.class)
            .put("attributeLong", ATTRIBUTE, Long.class)
            .put("attributeInteger", ATTRIBUTE, Long.class)
            .put("attributeFloat", ATTRIBUTE, Double.class)
            .put("attributeDouble", ATTRIBUTE, Double.class)
            .put("attributeBoolean", ATTRIBUTE, Boolean.class)
            .put("attributeInstant", ATTRIBUTE, Instant.class)

            .build();

    @Test
    public void testSerialize() throws IOException {

        DataStructureSerializer serializer = new DataStructureSerializer();
        StringWriter writer = new StringWriter();
        ObjectMapper mapper = new ObjectMapper();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        serializer.serialize(STRUCTURE, generator, null);
        generator.flush();

        assertThat(writer.toString()).isEqualTo("" +
                "[" +
                "{\"name\":\"idString\",\"role\":\"IDENTIFIER\",\"type\":\"STRING\"}," +
                "{\"name\":\"idLong\",\"role\":\"IDENTIFIER\",\"type\":\"INTEGER\"}," +
                "{\"name\":\"idInteger\",\"role\":\"IDENTIFIER\",\"type\":\"INTEGER\"}," +
                "{\"name\":\"idFloat\",\"role\":\"IDENTIFIER\",\"type\":\"NUMERIC\"}," +
                "{\"name\":\"idDouble\",\"role\":\"IDENTIFIER\",\"type\":\"NUMERIC\"}," +
                "{\"name\":\"idBoolean\",\"role\":\"IDENTIFIER\",\"type\":\"BOOLEAN\"}," +
                "{\"name\":\"idInstant\",\"role\":\"IDENTIFIER\",\"type\":\"DATE\"}," +
                "{\"name\":\"measureString\",\"role\":\"MEASURE\",\"type\":\"STRING\"}," +
                "{\"name\":\"measureLong\",\"role\":\"MEASURE\",\"type\":\"INTEGER\"}," +
                "{\"name\":\"measureInteger\",\"role\":\"MEASURE\",\"type\":\"INTEGER\"}," +
                "{\"name\":\"measureFloat\",\"role\":\"MEASURE\",\"type\":\"NUMERIC\"}," +
                "{\"name\":\"measureDouble\",\"role\":\"MEASURE\",\"type\":\"NUMERIC\"}," +
                "{\"name\":\"measureBoolean\",\"role\":\"MEASURE\",\"type\":\"BOOLEAN\"}," +
                "{\"name\":\"measureInstant\",\"role\":\"MEASURE\",\"type\":\"DATE\"}," +
                "{\"name\":\"attributeString\",\"role\":\"ATTRIBUTE\",\"type\":\"STRING\"}," +
                "{\"name\":\"attributeLong\",\"role\":\"ATTRIBUTE\",\"type\":\"INTEGER\"}," +
                "{\"name\":\"attributeInteger\",\"role\":\"ATTRIBUTE\",\"type\":\"INTEGER\"}," +
                "{\"name\":\"attributeFloat\",\"role\":\"ATTRIBUTE\",\"type\":\"NUMERIC\"}," +
                "{\"name\":\"attributeDouble\",\"role\":\"ATTRIBUTE\",\"type\":\"NUMERIC\"}," +
                "{\"name\":\"attributeBoolean\",\"role\":\"ATTRIBUTE\",\"type\":\"BOOLEAN\"}," +
                "{\"name\":\"attributeInstant\",\"role\":\"ATTRIBUTE\",\"type\":\"DATE\"}" +
                "]" +
                "");
    }
}