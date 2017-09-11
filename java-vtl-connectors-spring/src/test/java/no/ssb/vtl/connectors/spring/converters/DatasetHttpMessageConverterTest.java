package no.ssb.vtl.connectors.spring.converters;

/*-
 * ========================LICENSE_START=================================
 * Java VTL Spring connector
 * %%
 * Copyright (C) 2017 Statistics Norway and contributors
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.assertj.core.api.JUnitSoftAssertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;
import org.springframework.mock.http.MockHttpInputMessage;
import org.springframework.mock.http.MockHttpOutputMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DatasetHttpMessageConverterTest {

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    private DatasetHttpMessageConverter converter;

    private Set<MediaType> unsupportedType = ImmutableSet.of(
            MediaType.parseMediaType("something/else"),
            MediaType.parseMediaType("application/json")
    );

    private Set<MediaType> supportedTypes = ImmutableSet.copyOf(
            DatasetHttpMessageConverter.SUPPORTED_TYPES
    );
    private ObjectMapper mapper;

    static HttpInputMessage loadFile(String name) throws IOException {
        InputStream stream = getResource(name).openStream();
        return new MockHttpInputMessage(stream);
    }

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        converter = new DatasetHttpMessageConverter(mapper);
    }

    @Test
    public void testReadDatasetVersion2() throws Exception {
        HttpInputMessage message = loadFile("ssb.dataset+json;version=2" + ".json");

        Dataset result = (Dataset) converter.read(Dataset.class, message);
        System.out.println(result);
    }

    @Test
    public void testReadDataStructureVersion2() throws Exception {
        HttpInputMessage message = loadFile("ssb.dataset+json;version=2" + ".json");

        DataStructure result = (DataStructure) converter.read(DataStructure.class, message);
        System.out.println(result);
    }

    @Test
    public void testReadDataVersion2() throws Exception {
        HttpInputMessage message = loadFile("ssb.dataset+json;version=2" + ".json");

        TypeReference<Stream<DataPoint>> TYPE = new TypeReference<Stream<DataPoint>>() {
        };
        Stream<DataPoint> result = (Stream<DataPoint>) converter.read(TYPE.getType(), null, message);
        System.out.println(result);
    }

    @Test
    public void testStructureOrderIsConsistent() throws IOException {

        DataStructure structure1 = DataStructure.builder()
                .put("Id1", IDENTIFIER, String.class)
                .put("Me2", MEASURE, String.class)
                .put("Id2", IDENTIFIER, String.class)
                .put("At2", ATTRIBUTE, String.class)
                .put("Me1", MEASURE, String.class)
                .put("At1", ATTRIBUTE, String.class)
                .build();

        DataStructure structure2 = DataStructure.builder()
                .put("At2", ATTRIBUTE, String.class)
                .put("Me2", MEASURE, String.class)
                .put("At1", ATTRIBUTE, String.class)
                .put("Me1", MEASURE, String.class)
                .put("Id2", IDENTIFIER, String.class)
                .put("Id1", IDENTIFIER, String.class)
                .build();

        Map<String, Object> data = ImmutableMap.<String, Object>builder()
                .put("Id1","Id1")
                .put("Id2","Id2")
                .put("Me1","Me1")
                .put("Me2","Me2")
                .put("At1","At1")
                .put("At2","At2")
                .build();

        TestDataset dataset1 = new TestDataset(structure1, Lists.newArrayList(data));
        TestDataset dataset2 = new TestDataset(structure2, Lists.newArrayList(data));

        MockHttpOutputMessage outputMessage1 = new MockHttpOutputMessage();
        MockHttpOutputMessage outputMessage2 = new MockHttpOutputMessage();

        converter.write(dataset1, DatasetHttpMessageConverter.APPLICATION_DATASET_JSON, outputMessage1);
        converter.write(dataset2, DatasetHttpMessageConverter.APPLICATION_DATASET_JSON, outputMessage2);

        assertThat(outputMessage1.getBodyAsString()).isEqualTo(outputMessage2.getBodyAsString());

    }

    @Test
    public void testWriteEmptyDatasetVersion2() throws Exception {

        HttpInputMessage message = loadFile("ssb.dataset+json;version=2" + ".empty.json");
        Dataset result = (Dataset) converter.read(Dataset.class, message);

        MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();
        converter.write(result, DatasetHttpMessageConverter.APPLICATION_DATASET_JSON, outputMessage);

        // Go full circle!
        JsonNode original = mapper.readTree(loadFile("ssb.dataset+json;version=2" + ".empty.json").getBody());
        JsonNode written = mapper.readTree(outputMessage.getBodyAsBytes());

        assertThat(written).isEqualTo(original);
    }

    @Test
    public void testSerializationFail() throws IOException {

        DataStructure structure = DataStructure.builder().put("id", IDENTIFIER, String.class).build();
        LinkedHashMap<String, Object> data = Maps.newLinkedHashMap();

        // Failing stream.
        TestDataset dataset = new TestDataset(structure, null);

        MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();

        assertThatThrownBy(() -> {
            converter.write(dataset, DatasetHttpMessageConverter.APPLICATION_DATASET_JSON, outputMessage);
        })
                .isExactlyInstanceOf(JsonGenerationException.class)
                .hasCauseExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining("Failed to serialize dataset");


    }

    @Test
    public void testWriteDatasetVersion2() throws Exception {

        HttpInputMessage message = loadFile("ssb.dataset+json;version=2" + ".json");
        Dataset result = (Dataset) converter.read(Dataset.class, message);

        MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();
        converter.write(result, DatasetHttpMessageConverter.APPLICATION_DATASET_JSON, outputMessage);

        // Go full circle!
        JsonNode original = mapper.readTree(loadFile("ssb.dataset+json;version=2" + ".json").getBody());
        JsonNode written = mapper.readTree(outputMessage.getBodyAsBytes());

        assertThat(written).isEqualTo(original);
    }

    private static class TestDataset implements Dataset {

        private final DataStructure structure;
        private final List<Map<String, Object>> data;

        public TestDataset(DataStructure structure, List<Map<String, Object>> data) {
            this.structure = structure;
            this.data = data;
        }

        @Override
        public Stream<DataPoint> getData() {
            return data.stream().map(structure::fromStringMap);
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

    private static class ExtendedDataStructure extends DataStructure {
        protected ExtendedDataStructure(BiFunction<Object, Class<?>, ?> converter, ImmutableMap<String, Component> map) {
            super(converter, map);
        }
    }
}
