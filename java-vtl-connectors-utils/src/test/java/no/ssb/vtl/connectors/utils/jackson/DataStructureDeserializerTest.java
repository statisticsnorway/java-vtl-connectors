package no.ssb.vtl.connectors.utils.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.vtl.model.DataStructure;
import org.junit.Test;

import java.io.IOException;

import static no.ssb.vtl.connectors.utils.jackson.DataStructureSerializerTest.STRUCTURE_NORMALIZED;
import static org.assertj.core.api.Assertions.assertThat;

public class DataStructureDeserializerTest {

    @Test
    public void testDeserialize() throws IOException {
        DataStructureDeserializer deserializer = new DataStructureDeserializer();
        ObjectMapper mapper = new ObjectMapper();
        JsonParser parser = mapper.getFactory().createParser("[" +
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
                "]");

        // Ensure precondition holds upon deserialize() entry.
        parser.nextToken();

        DataStructure structure = deserializer.deserialize(parser, null);

        // Checks against normalized (no float nor int).
        assertThat(structure.keySet()).isEqualTo(STRUCTURE_NORMALIZED.keySet());
        assertThat(structure.getTypes()).isEqualTo(STRUCTURE_NORMALIZED.getTypes());
        assertThat(structure.getRoles()).isEqualTo(STRUCTURE_NORMALIZED.getRoles());
    }
}