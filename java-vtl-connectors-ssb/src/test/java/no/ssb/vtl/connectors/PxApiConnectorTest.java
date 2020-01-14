package no.ssb.vtl.connectors;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import no.ssb.vtl.connectors.util.IdentifierConverter;
import no.ssb.vtl.model.Component.Role;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;

import java.io.InputStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

public class PxApiConnectorTest {

    private MockRestServiceServer mockServer;
    private PxApiConnector connector;

    @Before
    public void setUp() {
        connector = new PxApiConnector(Lists.newArrayList("http://data.ssb.no/api/v0/no/table/"));
        mockServer = MockRestServiceServer.createServer(connector.getRestTemplate());
    }

    @Test
    public void testExtractVariables() {
        Set<String> variables = connector.extractVariables(
                "Region=all(*)&Eierskap=01+02-03+98&ContentsCode=Antall1&Tid=top(3)");
        assertThat(variables).containsExactlyInAnyOrder("Region", "Eierskap", "ContentsCode", "Tid");
    }

    @Test
    public void testBuildMissingQuery() {
        Set<String> identifiers = ImmutableSet.of("Region", "Eierskap", "Tid");
        Set<String> variables = ImmutableSet.of("Region");

        String query = connector.buildMissingQuery(identifiers, variables);
        assertThat(query).isEqualTo("Eierskap=all(*)&Tid=all(*)&ContentsCode=all(*)");

        Set<String> variablesWithContentsCodes = ImmutableSet.of("Region", "ContentsCode");
        query = connector.buildMissingQuery(identifiers, variablesWithContentsCodes);
        assertThat(query).isEqualTo("Eierskap=all(*)&Tid=all(*)");
    }

    @Test
    public void getDataset() throws Exception {
        InputStream fileStream = Resources.getResource(this.getClass(), "/09220.json").openStream();
        InputStream metaStream = Resources.getResource(this.getClass(), "/09220-metadata.json").openStream();
        InputStream queryStream = Resources.getResource(this.getClass(), "/query.json").openStream();
        JsonNode queryJson = new ObjectMapper().readTree(queryStream);

        mockServer.expect(requestTo("http://data.ssb.no/api/v0/no/table/09220"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(
                        new InputStreamResource(metaStream), MediaType.APPLICATION_JSON));

        mockServer.expect(requestTo("http://data.ssb.no/api/v0/no/table/09220"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().string(queryJson.toString()))
                .andRespond(withSuccess(
                        new InputStreamResource(fileStream),
                        MediaType.APPLICATION_JSON)
                );

        Dataset dataset = connector.getDataset("http://data.ssb.no/api/v0/no/table/09220?" +
                IdentifierConverter.toQueryString(queryJson.toString()));
        assertThat(dataset).isNotNull();

        assertThat(dataset.getDataStructure().getRoles()).containsOnly(
                entry("Region", Role.IDENTIFIER),
                entry("Eierskap", Role.IDENTIFIER),
                entry("Antall1", Role.MEASURE),
                entry("Tid", Role.IDENTIFIER)
        );

        assertThat(dataset.getDataStructure().getTypes()).containsOnly(
                entry("Region", String.class),
                entry("Eierskap", String.class),
                entry("Antall1", Double.class),
                entry("Tid", String.class)
        );

        assertThat(dataset.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsSequence(
                        "0", "01", "2015", 2821L,
                        "0", "01", "2016", 2774L,
                        "0", "01", "2017", 2722L,
                        "0", "02-03", "2015", 15L
                );
    }
}























