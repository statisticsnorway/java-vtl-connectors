package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.http.HttpInputMessage;

import java.time.Instant;
import java.util.function.BiFunction;

import static no.ssb.vtl.tools.sandbox.connector.spring.converters.DataStructureHttpConverter.MEDIA_TYPE;
import static no.ssb.vtl.tools.sandbox.connector.spring.converters.DatasetHttpMessageConverterTest.loadFile;

public class DataStructureHttpConverterTest {

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    private DataStructureHttpConverter converter;
    private ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        converter = new DataStructureHttpConverter(mapper);
    }

    @Test
    public void testReadDataStructureVersion2() throws Exception {
        HttpInputMessage message = loadFile("ssb.dataset.structure+json;version=2" + ".json");

        DataStructure result = converter.read(DataStructure.class, message);

        softly.assertThat(result)
                .as("name of the variables")
                .containsKeys(
                        "Country", "SentOn", "Measure", "Type", "Valid"
                );

        softly.assertThat(result.values())
                .extracting(Component::getType)
                .as("types of the variables")
                .containsExactly(
                        (Class) String.class,
                        (Class) Instant.class,
                        (Class) Double.class,
                        (Class) Long.class,
                        (Class) Boolean.class
                );

        softly.assertThat(result.values())
                .extracting(Component::getRole)
                .as("types of the variables")
                .containsExactly(
                        Component.Role.IDENTIFIER,
                        Component.Role.ATTRIBUTE,
                        Component.Role.MEASURE,
                        Component.Role.ATTRIBUTE,
                        Component.Role.ATTRIBUTE
                );
    }

    @Test
    public void testCanRead() throws Exception {
        softly.assertThat(

                converter.canRead(DataStructure.class, MEDIA_TYPE)

        ).as(
                "supports reading class %s with media type %s",
                DataStructure.class, MEDIA_TYPE
        ).isTrue();

        softly.assertThat(

                converter.canRead(ExtendedDataStructure.class, MEDIA_TYPE)

        ).as(
                "supports reading subclass %s with media type %s",
                ExtendedDataStructure.class, MEDIA_TYPE
        ).isFalse();
    }

    @Test
    public void canWrite() throws Exception {
        softly.assertThat(

                converter.canWrite(DataStructure.class, MEDIA_TYPE)

        ).as(
                "supports writing class %s with media type %s",
                DataStructure.class, MEDIA_TYPE
        ).isTrue();

        softly.assertThat(

                converter.canWrite(ExtendedDataStructure.class, MEDIA_TYPE)

        ).as(
                "supports writing subclass %s with media type %s",
                ExtendedDataStructure.class, MEDIA_TYPE
        ).isTrue();
    }

    private static class ExtendedDataStructure extends DataStructure {
        protected ExtendedDataStructure(BiFunction<Object, Class<?>, ?> converter, ImmutableMap<String, Component> map) {
            super(converter, map);
        }
    }
}
