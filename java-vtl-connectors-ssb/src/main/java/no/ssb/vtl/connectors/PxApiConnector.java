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
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import no.ssb.jsonstat.v2.DatasetBuildable;
import no.ssb.vtl.connectors.px.DataStructureDeserializer;
import no.ssb.vtl.connectors.util.IdentifierConverter;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.DatapointNormalizer;
import no.ssb.vtl.model.Dataset;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.AsyncClientHttpRequest;
import org.springframework.http.client.AsyncClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.client.AsyncRestTemplate;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class PxApiConnector extends JsonStatConnector {

    private static final ParameterizedTypeReference<Map<String, DatasetBuildable>> TYPE_REFERENCE =
            new ParameterizedTypeReference<Map<String, DatasetBuildable>>() {
            };
    private final AsyncRestTemplate asyncRestTemplate;

    private final List<String> baseUrls;

    public PxApiConnector(List<String> baseUrls) {
        checkNotNull(baseUrls);
        this.baseUrls = baseUrls;

        // Add support for structure deserialization.
        SimpleModule module = new SimpleModule();
        module.addDeserializer(DataStructure.class, new DataStructureDeserializer());
        this.mapper.registerModule(module);

        RestTemplate originalTemplate = getRestTemplate();
        SimpleClientHttpRequestFactory asyncClientHttpRequestFactory =
                (SimpleClientHttpRequestFactory) originalTemplate.getRequestFactory();
        asyncClientHttpRequestFactory.setTaskExecutor(new ConcurrentTaskExecutor(
                Executors.newCachedThreadPool()
        ));
        asyncRestTemplate = new AsyncRestTemplate(
                asyncClientHttpRequestFactory,
                originalTemplate
        );

    }

    @Override
    public boolean canHandle(String identifier) {
        for (String baseUrl : baseUrls) {
            if (identifier.startsWith(baseUrl)) {
                return true;
            }
        }
        return false;
    }

    public DataStructure fetchStructure(URI uri) throws ConnectorException {
        try {
            return getRestTemplate().getForObject(removeQuery(uri), DataStructure.class);
        } catch (RestClientResponseException rcre) {
            throw new ConnectorException(
                    format("fetching metadata from %s returned %s", uri, rcre.getRawStatusCode()),
                    rcre
            );
        } catch (ResourceAccessException rae) {
            throw new ConnectorException(
                    format("could not contact %s", uri),
                    rae
            );
        } catch (Exception e) {
            throw new ConnectorException("unknown error", e);
        }
    }

    public ListenableFuture<ResponseEntity<Map<String, DatasetBuildable>>> fetchData(URI uri, DataStructure structure) throws ConnectorException {
        try {
            return asyncRestTemplate.exchange(
                    removeQuery(uri), HttpMethod.POST, createEntity(uri, structure), TYPE_REFERENCE);
        } catch (RestClientResponseException rcre) {
            throw new ConnectorException(
                    format("fetching metadata from %s returned %s", uri, rcre.getRawStatusCode()),
                    rcre
            );
        } catch (ResourceAccessException rae) {
            throw new ConnectorException(
                    format("could not contact %s", uri),
                    rae
            );
        }
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        try {
            URI uri = new URI(identifier);
            DataStructure structure = fetchStructure(uri);
            return new AsyncDataset(structure, fetchData(uri, structure));
        } catch (URISyntaxException use) {
            throw new ConnectorException(format("invalid uri %s", identifier), use);
        } catch (Exception e) {
            if (e instanceof ConnectorException) {
                throw e;
            } else {
                throw new ConnectorException(format("unknown error getting dataset %s", identifier), e);
            }
        }
    }

    private URI removeQuery(URI uri) throws ConnectorException {
        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(),
                    uri.getPort(), uri.getPath(), null, uri.getFragment());
        } catch (URISyntaxException use) {
            throw new ConnectorException(format("could not remove query from %s", uri));
        }

    }

    private HttpEntity<JsonNode> createEntity(URI uri, DataStructure structure) throws ConnectorException {
        String query = uri.getQuery();
        if (query == null) {
            throw new ConnectorException(format("missing query in %s", uri));
        }
        // Px api can decide not to return all the identifier variables if they are marked with "elimination".
        // Here we make sure that all the identifier in the query are present.
        Set<String> queryVariables = Stream.of(uri.getQuery().split("&"))
                .filter(s -> s.contains("="))
                .map(s -> s.split("=")[0])
                .collect(Collectors.toSet());

        Sets.SetView<String> missingVariables = Sets.difference(structure.keySet(), queryVariables);
        String missingQuery = Stream.concat(missingVariables.stream()
                .filter(s -> !s.isEmpty())
                .filter(s -> structure.get(s).isIdentifier())
                .filter(s -> !queryVariables.contains(s)), queryVariables.contains("ContentsCode") ? Stream.empty() : Stream.of("ContentsCode"))
                .map(s -> s.concat("=all(*)"))
                .collect(Collectors.joining("&"));

        try {
            return new HttpEntity<>(IdentifierConverter.toJson(String.join("&", query, missingQuery)));
        } catch (Exception e) {
            throw new ConnectorException(format("could not create query from %s", uri), e);
        }

    }

    private class AsyncDataset implements Dataset {

        private final DataStructure structure;
        private final Future<ResponseEntity<Map<String, DatasetBuildable>>> dataset;

        private AsyncDataset(DataStructure structure, Future<ResponseEntity<Map<String, DatasetBuildable>>> dataset) {
            this.structure = checkNotNull(structure);
            this.dataset = checkNotNull(dataset);
        }

        @Override
        public Stream<DataPoint> getData() {
            try {
                Dataset dataset = buildDataset(this.dataset.get());
                return dataset.getData().map(new DatapointNormalizer(dataset.getDataStructure(), getDataStructure()));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
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

}
