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
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import org.junit.Test;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import no.ssb.vtl.connectors.spring.converters.DataHttpConverter;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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

        Dataset dataset = restTemplateConnector.getDataset("http://www.mocky.io/v2/594a48ee10000081031aa3fc");
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

        Order order = Order.create(structure)
                .put("id1", Order.Direction.ASC)
                .put("id3", Order.Direction.DESC)
                .put("id2", Order.Direction.DESC)
                .put("id4", Order.Direction.ASC)
                .build();


        UriComponentsBuilder orderUri = RestTemplateConnector.createOrderUri(uri, order, structure);

        assertThat(orderUri.build().toUriString())
                .contains("sort=id1,ASC&sort=id3,id2,DESC&sort=id4,ASC")
                .contains("foo=bar&foo2=bar2");

    }

    @Test
    public void testOrderWithExistingSort() throws Exception {

        UriComponentsBuilder uri = UriComponentsBuilder.fromHttpUrl("http://test/api/id?foo=bar&foo2=bar2");

        DataStructure structure = DataStructure.builder()
                .put("id1", Component.Role.IDENTIFIER, String.class)
                .put("id2", Component.Role.IDENTIFIER, String.class)
                .put("id3", Component.Role.IDENTIFIER, String.class)
                .put("id4", Component.Role.IDENTIFIER, String.class)
                .build();

        Order order = Order.create(structure)
                .put("id1", Order.Direction.ASC)
                .put("id3", Order.Direction.DESC)
                .put("id2", Order.Direction.DESC)
                .put("id4", Order.Direction.ASC)
                .build();

        UriComponentsBuilder uriWithOrder = UriComponentsBuilder.fromHttpUrl("http://test/api/id?sort=foo&other=param");

        UriComponentsBuilder orderUri = RestTemplateConnector.createOrderUri(uriWithOrder.cloneBuilder().cloneBuilder(), order, structure);

        assertThat(orderUri.build().toUriString())
                .contains("sort=id1,ASC&sort=id3,id2,DESC&sort=id4,ASC")
                .doesNotContain("sort=foo")
                .contains("other=param");

    }

}
