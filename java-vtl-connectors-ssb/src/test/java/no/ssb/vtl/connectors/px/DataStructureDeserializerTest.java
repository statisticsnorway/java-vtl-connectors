package no.ssb.vtl.connectors.px;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.io.Resources;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

public class DataStructureDeserializerTest {

    private ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(DataStructure.class, new DataStructureDeserializer());
        mapper.registerModule(module);
    }

    @Test
    public void testMetadataParsing() throws IOException {
        URL resource = Resources.getResource(this.getClass(), "/metadata.json");
        try (InputStream metadata = resource.openStream()) {

            DataStructure structure = mapper.readValue(metadata, DataStructure.class);
            assertThat(structure.keySet()).containsExactly(
                    "KOKeieform0000",
                    "KOKfunksjon0000",
                    "KOKkommuneregion0000",
                    "Tid",
                    "KOSareal0000"
            );

            assertThat(structure.getRoles()).containsValues(
                    Component.Role.IDENTIFIER,
                    Component.Role.IDENTIFIER,
                    Component.Role.IDENTIFIER,
                    Component.Role.IDENTIFIER,
                    Component.Role.MEASURE
            );

            assertThat(structure.getTypes()).containsValues(
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Double.class
            );
        }
    }

}