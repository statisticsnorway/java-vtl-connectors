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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import no.ssb.jsonstat.JsonStatModule;
import no.ssb.jsonstat.v2.DatasetBuildable;
import no.ssb.jsonstat.v2.Dimension;
import no.ssb.jsonstat.v2.Dimension.Roles;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.StaticDataset;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public abstract class JsonStatConnector implements Connector {
    private final ObjectMapper mapper;
    private final RestTemplate restTemplate;

    public JsonStatConnector(ObjectMapper mapper) {

        this.mapper = checkNotNull(mapper, "the mapper was null").copy();

        this.mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        this.mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

        this.mapper.registerModule(new GuavaModule());
        this.mapper.registerModule(new Jdk8Module());
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.registerModule(new JsonStatModule());

        ResourceHttpMessageConverter resourceConverter = new ResourceHttpMessageConverter();
        MappingJackson2HttpMessageConverter jacksonConverter;
        jacksonConverter = new MappingJackson2HttpMessageConverter(this.mapper);

        this.restTemplate = new RestTemplate(asList(
                resourceConverter,
                jacksonConverter
        ));


    }

    public JsonStatConnector() {
        this(new ObjectMapper());
    }

    /**
     * Gives access to the rest template to tests.
     */
    RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public abstract boolean canHandle(String identifier);

    public abstract Dataset getDataset(String identifier) throws ConnectorException;

    protected Dataset buildDataset(ResponseEntity<Map<String, DatasetBuildable>> exchange) {
        no.ssb.jsonstat.v2.Dataset dataset = exchange.getBody().values().iterator().next().build();

        Map<String, Dimension> dimensions = dataset.getDimension();

        ImmutableMultimap<Roles, String> role = dataset.getRole();
        Set<String> metric = ImmutableSet.copyOf(role.get(Dimension.Roles.METRIC));
        Set<String> ids = Sets.symmetricDifference(dataset.getId(), metric);

        Set<String> rotatedMetricName = computeMetricNames(dimensions, metric);
        DataStructure structure = generateStructure(ids, rotatedMetricName);


        Table<List<String>, List<String>, Number> table = dataset.asTable(ids, metric);

        Map<String, Object> buffer = Maps.newHashMap();
        StaticDataset.ValueBuilder datasetBuilder = StaticDataset.create(structure);
        for (Map.Entry<List<String>, Map<List<String>, Number>> entry : table.rowMap().entrySet()) {
            buffer.clear();
            Iterator<String> identifierValues = entry.getKey().iterator();
            for (String id : ids) {
                buffer.put(id, identifierValues.next());
            }
            for (Map.Entry<List<String>, Number> measures : entry.getValue().entrySet()) {
                String name = String.join("_", measures.getKey());
                Number value = measures.getValue();
                buffer.put(name, value);
            }
            datasetBuilder.addPoints(structure.wrap(buffer));
        }

        return datasetBuilder.build();
    }

    private Set<String> computeMetricNames(Map<String, Dimension> dimensions, Set<String> metric) {
        List<Set<String>> metricValues = Lists.newArrayList();
        for (String metricName : metric) {
            metricValues.add(dimensions.get(metricName).getCategory().getIndex());
        }
        return Sets.cartesianProduct(metricValues).stream().map(
                strings -> String.join("_", strings)
        ).collect(Collectors.toSet());
    }

    private DataStructure generateStructure(Set<String> ids, Set<String> metrics) {
        Map<String, Component.Role> roles = Maps.newHashMap();
        Map<String, Class<?>> types = Maps.newHashMap();
        for (String name : ids) {
            roles.put(name, Component.Role.IDENTIFIER);
            types.put(name, String.class);
        }
        for (String name : metrics) {
            roles.put(name, Component.Role.MEASURE);
            types.put(name, Double.class); //TODO: Use dimension.category.unit.decimals to determine type (Long or Double)
        }
        return DataStructure.of(types, roles);
    }

    public Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException {
        throw new ConnectorException("not supported");
    }
}

