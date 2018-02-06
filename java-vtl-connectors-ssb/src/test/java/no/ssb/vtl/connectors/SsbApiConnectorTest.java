package no.ssb.vtl.connectors;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
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
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

public class SsbApiConnectorTest {

    private ObjectMapper mapper;
    private Connector connector;
    private MockRestServiceServer mockServer;

    @Before
    public void setUp() throws Exception {
        this.mapper = new ObjectMapper();
        SsbApiConnector ssbConnector = new SsbApiConnector(this.mapper);
        this.connector = ssbConnector;
        mockServer = MockRestServiceServer.createServer(ssbConnector.getRestTemplate());
    }

    @Test
    public void testGetDataset() throws Exception {

        InputStream fileStream = Resources.getResource(this.getClass(), "/1106.json").openStream();

        mockServer.expect(requestTo("http://data.ssb.no/api/v0/dataset/1106.json?lang=en"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(
                        new InputStreamResource(fileStream),
                        MediaType.APPLICATION_JSON)
                );

        Dataset dataset = this.connector.getDataset("1106");

        assertThat(dataset.getDataStructure().getRoles()).containsOnly(
                entry("Dode3", Component.Role.MEASURE),
                entry("Fodselsoverskudd4", Component.Role.MEASURE),
                entry("Fodte2", Component.Role.MEASURE),
                entry("Folketallet11", Component.Role.MEASURE),
                entry("Folketilvekst10", Component.Role.MEASURE),
                entry("Fraflytting8", Component.Role.MEASURE),
                entry("Innvandring5", Component.Role.MEASURE),
                entry("Nettoinnflytting9", Component.Role.MEASURE),
                entry("Region", Component.Role.IDENTIFIER),
                entry("Tid", Component.Role.IDENTIFIER),
                entry("Tilflytting7", Component.Role.MEASURE),
                entry("Utvandring6", Component.Role.MEASURE)
        );

        assertThat(dataset.getDataStructure().getTypes()).containsOnly(
                entry("Dode3", Double.class),
                entry("Fodselsoverskudd4", Double.class),
                entry("Fodte2", Double.class),
                entry("Folketallet11", Double.class),
                entry("Folketilvekst10", Double.class),
                entry("Fraflytting8", Double.class),
                entry("Innvandring5", Double.class),
                entry("Nettoinnflytting9", Double.class),
                entry("Region", String.class),
                entry("Tid", String.class),
                entry("Tilflytting7", Double.class),
                entry("Utvandring6", Double.class)
        );

        assertThat(dataset.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsSequence(
                        30308L, 75L, 79L, 72L, "0101", 70L, 26L, 82L, "2014K3",
                        387L, -3L, 425L, 30328L, 95L, 20L, 63L, "0101", 42L, 68L, 49L, "2014K4"
                );

    }


}
