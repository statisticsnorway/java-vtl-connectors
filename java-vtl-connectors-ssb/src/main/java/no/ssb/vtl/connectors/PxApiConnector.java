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
import com.google.common.annotations.VisibleForTesting;
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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.client.AsyncRestTemplate;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;

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
        SimpleModule module = new SimpleModule("SSB structure deserializer");
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
            throw new ConnectorException(format("could not contact %s", uri), rae);
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
                    format("fetching data from %s returned %s", uri, rcre.getRawStatusCode()),
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
            // Since we are building the new URI from a URI object, this exception
            // really should never occur.
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

        Set<String> queryVariables = extractVariables(uri.getQuery());

        Set<String> identifiers = structure.getRoles().entrySet()
                .stream()
                .filter(e -> IDENTIFIER.equals(e.getValue()))
                .map(e -> e.getKey())
                .collect(Collectors.toSet());

        String missingQuery = buildMissingQuery(identifiers, queryVariables);

        try {
            return new HttpEntity<>(IdentifierConverter.toJson(String.join("&", query, missingQuery)));
        } catch (Exception e) {
            throw new ConnectorException(format("could not create query from %s", uri), e);
        }

    }

    @VisibleForTesting
    String buildMissingQuery(Set<String> structure, Set<String> queryVariables) {
        Set<String> structureWithContent = Sets.union(structure, ImmutableSet.of("ContentsCode"));
        Sets.SetView<String> missingVariables = Sets.difference(structureWithContent, queryVariables);
        return missingVariables.stream().map(s -> s.concat("=all(*)")).collect(Collectors.joining("&"));
    }

    @VisibleForTesting
    Set<String> extractVariables(String query) {
        return Stream.of(query.split("&"))
                .filter(s -> s.contains("="))
                .map(s -> s.split("=")[0])
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
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
            return StreamSupport.stream(this::spliterator, Spliterator.IMMUTABLE, false);
        }

        private Spliterator<DataPoint> spliterator() {
            try {
                Dataset dataset = buildDataset(this.dataset.get());
                DatapointNormalizer normalizer = new DatapointNormalizer(dataset.getDataStructure(), getDataStructure());
                return dataset.getData()
                        .map(normalizer)
                        .spliterator();
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
