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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static no.ssb.vtl.model.VtlOrdering.BY_NAME;
import static no.ssb.vtl.model.VtlOrdering.BY_ROLE;

/**
 * A converter that can read and write data structures.
 * <p>
 * It supports reading from:
 * <ul>
 * <li>application/ssb.dataset+json</li>
 * <li>application/x-ssb.dataset+json</li>
 * <li>application/ssb.dataset.structure+json</li>
 * <li>application/x-ssb.dataset.structure+json</li>
 * </ul>
 * <p>
 * And writes:
 * <ul>
 * <li>application/ssb.dataset.structure+json</li>
 * <li>application/x-ssb.dataset.structure+json</li>
 * </ul>
 */
public class DataStructureHttpConverter extends AbstractHttpMessageConverter<DataStructure> {

    public static final String APPLICATION_SSB_DATASET_STRUCTURE_JSON_VALUE = "application/ssb.dataset.structure+json";
    public static final MediaType APPLICATION_SSB_DATASET_STRUCTURE_JSON = MediaType.parseMediaType(APPLICATION_SSB_DATASET_STRUCTURE_JSON_VALUE);

    private final ObjectMapper mapper;

    private final TypeReference<List<DataStructureWrapper>> TYPE_REFERENCE = new TypeReference<List<DataStructureWrapper>>() {
    };

    protected DataStructureHttpConverter(MediaType supportedMediaType, ObjectMapper mapper) {
        super(supportedMediaType);
        this.mapper = checkNotNull(mapper);
    }

    public DataStructureHttpConverter(ObjectMapper mapper) {
        this(APPLICATION_SSB_DATASET_STRUCTURE_JSON, mapper);
    }

    /**
     * Sort the given DataStructure by role and then name.
     */
    static DataStructure sortDataStructure(DataStructure structure) {
        TreeSet<Map.Entry<String, Component>> sortedEntrySet = Sets.newTreeSet(BY_ROLE.thenComparing(BY_NAME));
        sortedEntrySet.addAll(structure.entrySet());
        return DataStructure.builder().putAll(sortedEntrySet).build();
    }

    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return clazz.isAssignableFrom(DataStructure.class) && canRead(mediaType);
    }

    @Override
    public boolean canWrite(Class<?> clazz, MediaType mediaType) {
        return canWrite(mediaType) && DataStructure.class.isAssignableFrom(clazz);
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        throw new UnsupportedOperationException(); // we rely on can read and can write.
    }

    DataStructure readWithParser(JsonParser parser) throws IOException {
        List<DataStructureWrapper> parsed = parser.readValueAs(TYPE_REFERENCE);

        DataStructure.Builder builder = DataStructure.builder();
        for (DataStructureWrapper variable : parsed) {
            builder.put(
                    variable.getName(),
                    variable.getRole(),
                    variable.getType().getType()
            );
        }
        return builder.build();
    }

    @Override
    protected DataStructure readInternal(Class<? extends DataStructure> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        JsonParser parser = mapper.getFactory().createParser(inputMessage.getBody());
        return readWithParser(parser);
    }

    @Override
    protected void writeInternal(DataStructure dataStructure, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        try (JsonGenerator generator = mapper.getFactory().createGenerator(outputMessage.getBody())) {
            generator.writeStartArray(dataStructure.size());
            writeWithParser(dataStructure, generator);
            generator.writeEndArray();
        }
    }

    void writeWithParser(DataStructure dataStructure, JsonGenerator generator) throws IOException {
        DataStructure sortedDatastructure = sortDataStructure(dataStructure);
        for (Map.Entry<String, Component> variable : sortedDatastructure.entrySet()) {
            Component component = variable.getValue();

            generator.writeStartObject();
            generator.writeStringField("name", variable.getKey());
            generator.writeStringField("role", component.getRole().name());
            generator.writeStringField("type", RoleMapping.fromType(component.getType()).name());
            generator.writeEndObject();
        }
    }

    private static class DataStructureWrapper {

        private String name;
        private Component.Role role;
        private RoleMapping type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Component.Role getRole() {
            return role;
        }

        public void setRole(Component.Role role) {
            this.role = role;
        }

        public RoleMapping getType() {
            return type;
        }

        public void setType(RoleMapping type) {
            this.type = type;
        }
    }
}
