package no.ssb.vtl.connectors.px;

/*-
 * ========================LICENSE_START=================================
 * Java VTL Utility connectors
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
    public void setUp() {
        mapper = new ObjectMapper();
        mapper.registerModule(new PxModule());
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