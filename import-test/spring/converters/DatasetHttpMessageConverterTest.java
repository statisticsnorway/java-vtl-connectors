package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;
import org.springframework.mock.http.MockHttpInputMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.io.Resources.getResource;
import static no.ssb.vtl.tools.sandbox.connector.spring.converters.DatasetHttpMessageConverter.SUPPORTED_TYPES;

public class DatasetHttpMessageConverterTest {

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    private DatasetHttpMessageConverter converter;

    private Set<MediaType> unsupportedType = ImmutableSet.of(
            MediaType.parseMediaType("something/else"),
            MediaType.parseMediaType("application/json")
    );

    private Set<MediaType> supportedTypes = ImmutableSet.copyOf(
            SUPPORTED_TYPES
    );

    static HttpInputMessage loadFile(String name) throws IOException {
        InputStream stream = getResource(name).openStream();
        return new MockHttpInputMessage(stream);
    }

    @Before
    public void setUp() throws Exception {
        converter = new DatasetHttpMessageConverter(new ObjectMapper());
    }

    @Test
    public void testReadDatasetVersion2() throws Exception {
        HttpInputMessage message = loadFile("ssb.dataset+json;version=2" + ".json");

        Dataset result = (Dataset) converter.read(DataStructure.class, message);
        System.out.println(result);
    }

    private static class ExtendedDataStructure extends DataStructure {
        protected ExtendedDataStructure(BiFunction<Object, Class<?>, ?> converter, ImmutableMap<String, Component> map) {
            super(converter, map);
        }
    }
}
