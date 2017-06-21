package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.reflect.TypeToken;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.VTLObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpInputMessage;
import org.springframework.mock.http.MockHttpInputMessage;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static java.time.Instant.parse;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;


public class DataHttpConverterTest {

    private DataHttpConverter converter;

    @Before
    public void setUp() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        converter = new DataHttpConverter(mapper);
    }

    @Test
    public void testReadDataVersion2() throws Exception {
        TypeToken<Stream<DataPoint>> typeToken = new TypeToken<Stream<DataPoint>>() {
        };

        HttpInputMessage message = loadFile("ssb.dataset.data+json;version=2.json");

        Stream<DataPoint> result = converter.read(typeToken.getType(), null, message);

        List<DataPoint> collect = result.collect(toList());
        assertThat(collect).isNotNull();


        assertThat(collect)
                .flatExtracting(input -> input)
                .filteredOn(vtlObject -> !VTLObject.NULL.equals(vtlObject))
                .extracting(input -> input.get().getClass())
                .containsExactly(
                        (Class) String.class, (Class) Instant.class, (Class) Double.class, (Class) Long.class, (Class) Boolean.class,
                        (Class) String.class, (Class) Instant.class, (Class) Double.class, (Class) Long.class, (Class) Boolean.class,
                        (Class) String.class, (Class) Instant.class, (Class) Double.class, (Class) Long.class, (Class) Boolean.class,
                        (Class) String.class, (Class) Instant.class, (Class) Double.class, (Class) Long.class, // null,
                        (Class) String.class, (Class) Instant.class, (Class) Double.class, (Class) Long.class  // null
                );

        assertThat(collect)
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "France", parse("2001-01-01T01:01:01.001Z"), 1.1, 1L, true,
                        "Norway", parse("2002-02-02T02:02:02.002Z"), 2.2, 2L, false,
                        "Sweden", parse("2003-03-03T03:03:03.003Z"), 3.3, 3L, true,
                        "Denmark", parse("2004-04-04T04:04:04.004Z"), 4.4, 4L, null,
                        "Italy", parse("2005-05-05T05:05:05.005Z"), 5.5, 5L, null
                );
    }

    private HttpInputMessage loadFile(String name) throws IOException {
        InputStream stream = getResource(name).openStream();
        return new MockHttpInputMessage(stream);
    }
}
