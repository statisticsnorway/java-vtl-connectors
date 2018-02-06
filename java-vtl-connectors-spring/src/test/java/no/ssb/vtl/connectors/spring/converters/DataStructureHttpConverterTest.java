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

public class DataStructureHttpConverterTest {

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    private DataStructureHttpConverter converter;
    private ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = new ObjectMapper();
        converter = new DataStructureHttpConverter(mapper);
    }

    @Test
    public void testReadDataStructureVersion2() throws Exception {
        HttpInputMessage message = DatasetHttpMessageConverterTest.loadFile("ssb.dataset.structure+json;version=2" + ".json");

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
                        (Class) Number.class,
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
                        Component.Role.MEASURE,
                        Component.Role.ATTRIBUTE,
                        Component.Role.ATTRIBUTE
                );
    }

    @Test
    public void testCanRead() {
        softly.assertThat(

                converter.canRead(DataStructure.class, DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON)

        ).as(
                "supports reading class %s with media type %s",
                DataStructure.class, DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON
        ).isTrue();

        softly.assertThat(

                converter.canRead(ExtendedDataStructure.class, DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON)

        ).as(
                "supports reading subclass %s with media type %s",
                ExtendedDataStructure.class, DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON
        ).isFalse();
    }

    @Test
    public void canWrite() {
        softly.assertThat(

                converter.canWrite(DataStructure.class, DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON)

        ).as(
                "supports writing class %s with media type %s",
                DataStructure.class, DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON
        ).isTrue();

        softly.assertThat(

                converter.canWrite(ExtendedDataStructure.class, DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON)

        ).as(
                "supports writing subclass %s with media type %s",
                ExtendedDataStructure.class, DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON
        ).isTrue();
    }

    private static class ExtendedDataStructure extends DataStructure {
        protected ExtendedDataStructure(BiFunction<Object, Class<?>, ?> converter, ImmutableMap<String, Component> map) {
            super(converter, map);
        }
    }
}
