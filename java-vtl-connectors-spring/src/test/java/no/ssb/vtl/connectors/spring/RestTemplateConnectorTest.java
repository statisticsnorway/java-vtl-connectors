package no.ssb.vtl.connectors.spring;

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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.ssb.vtl.connectors.spring.converters.DataHttpConverter;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VtlOrdering;
import org.junit.Test;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * Created by hadrien on 13/06/17.
 */
public class RestTemplateConnectorTest {

    @Test
    public void getData() throws Exception {

        // Setup the factory.
        SimpleClientHttpRequestFactory schrf = new SimpleClientHttpRequestFactory();
        schrf.setBufferRequestBody(false);
        schrf.setTaskExecutor(new SimpleAsyncTaskExecutor());

        schrf.setConnectTimeout(200);
        schrf.setReadTimeout(1000);

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        RestTemplate template = new RestTemplate(schrf);
        MockRestServiceServer mockServer = MockRestServiceServer.createServer(template);

        InputStream inputStream = getResource("ssb.dataset.data+json;version=2.json").openStream();
        InputStreamResource resource = new InputStreamResource(inputStream);
        mockServer.expect(
                requestTo("dataset")
        ).andExpect(
                method(HttpMethod.GET)
        ).andRespond(
                withSuccess(resource, MediaType.parseMediaType("application/ssb.dataset.data+json;version=2"))
        );

        template.getInterceptors().add(
                new AuthorizationTokenInterceptor()
        );

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        template.getMessageConverters().add(
                0, new DataHttpConverter(mapper)
        );

        RestTemplateConnector restTemplateConnector = new RestTemplateConnector(
                template,
                executorService
        );


        Dataset dataset = restTemplateConnector.getDataset("dataset");
        Stream<DataPoint> data = dataset.getData();
        data.forEach(System.out::println);

    }

    @Test
    public void testOrder() throws Exception {

        UriComponentsBuilder uri = UriComponentsBuilder.fromHttpUrl("http://test/api/id?foo=bar&foo2=bar2");

        DataStructure structure = DataStructure.builder()
                .put("id1", Component.Role.IDENTIFIER, String.class)
                .put("id2", Component.Role.IDENTIFIER, String.class)
                .put("id3", Component.Role.IDENTIFIER, String.class)
                .put("id4", Component.Role.IDENTIFIER, String.class)
                .build();

        VtlOrdering order = VtlOrdering.using(structure)
                .asc("id1")
                .desc("id3")
                .desc("id2")
                .asc("id4")
                .build();


        UriComponentsBuilder orderUri = RestTemplateConnector.createOrderUri(uri, order, structure);

        assertThat(orderUri.build().toUriString())
                .contains("sort=id1,ASC&sort=id3,id2,DESC&sort=id4,ASC")
                .contains("foo=bar&foo2=bar2");

    }

    @Test
    public void testOrderWithExistingSort() throws Exception {

        UriComponentsBuilder uriWithOrder = UriComponentsBuilder.fromHttpUrl("http://test/api/id?sort=foo&other=param");

        DataStructure structure = DataStructure.builder()
                .put("id1", Component.Role.IDENTIFIER, String.class)
                .put("id2", Component.Role.IDENTIFIER, String.class)
                .put("id3", Component.Role.IDENTIFIER, String.class)
                .put("id4", Component.Role.IDENTIFIER, String.class)
                .build();

        VtlOrdering order = VtlOrdering.using(structure)
                .asc("id1")
                .desc("id3")
                .desc("id2")
                .asc("id4")
                .build();

        UriComponentsBuilder orderUri = RestTemplateConnector.createOrderUri(uriWithOrder.cloneBuilder().cloneBuilder(), order, structure);

        assertThat(orderUri.build().toUriString())
                .contains("sort=id1,ASC&sort=id3,id2,DESC&sort=id4,ASC")
                .doesNotContain("sort=foo")
                .contains("other=param");

    }

}
