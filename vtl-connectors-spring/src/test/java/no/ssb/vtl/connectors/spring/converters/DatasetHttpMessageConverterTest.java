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

import com.fasterxml.jackson.databind.JsonNode;
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
import org.springframework.mock.http.MockHttpOutputMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

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
        converter = new DatasetHttpMessageConverter(mapper);
    }

    @Test
    public void testReadDatasetVersion2() throws Exception {
        HttpInputMessage message = loadFile("ssb.dataset+json;version=2" + ".json");

        Dataset result = (Dataset) converter.read(DataStructure.class, message);
        System.out.println(result);
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

    private static class ExtendedDataStructure extends DataStructure {
        protected ExtendedDataStructure(BiFunction<Object, Class<?>, ?> converter, ImmutableMap<String, Component> map) {
            super(converter, map);
        }
    }
}
