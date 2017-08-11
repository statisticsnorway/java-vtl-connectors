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
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLObject;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static no.ssb.vtl.connectors.spring.converters.DataHttpConverter.APPLICATION_SSB_DATASET_DATA_JSON_V2;
import static no.ssb.vtl.connectors.spring.converters.DataStructureHttpConverter.APPLICATION_SSB_DATASET_STRUCTURE_JSON;
import static no.ssb.vtl.model.Order.BY_NAME;
import static no.ssb.vtl.model.Order.BY_ROLE;

/**
 * A converter that support the following conversions
 * <p>
 * Read:
 * application/ssb.dataset+json;version=2 -> DataStructure
 * application/ssb.dataset+json;version=2 -> Stream<DataPoint>
 * <p>
 * Write:
 * <p>
 * Dataset -> application/ssb.dataset+json;version=2
 * Dataset -> application/ssb.dataset.data+json;version=2
 * Dataset -> application/ssb.dataset.structure+json;version=2
 */
public class DatasetHttpMessageConverter extends MappingJackson2HttpMessageConverter {

    public static final String APPLICATION_DATASET_JSON_VALUE = "application/ssb.dataset+json;version=2";

    public static final MediaType APPLICATION_DATASET_JSON = MediaType.parseMediaType(APPLICATION_DATASET_JSON_VALUE);

    public static final String STRUCTURE_FIELD_NAME = "structure";
    public static final String DATA_FIELD_NAME = "data";

    private final DataHttpConverter dataConverter;
    private final DataStructureHttpConverter structureConverter;

    // @formatter:off
    private static final TypeToken<Stream<DataPoint>> STREAM_TYPE_TOKEN = new TypeToken<Stream<DataPoint>>() {};
    private static final TypeReference<List<Object>> LIST_TYPE_REFERENCE = new TypeReference<List<Object>>() {};
    // @formatter:on

    @VisibleForTesting
    static final List<MediaType> SUPPORTED_TYPES;

    static {
        SUPPORTED_TYPES = new ArrayList<>();
        SUPPORTED_TYPES.add(APPLICATION_DATASET_JSON);
        SUPPORTED_TYPES.add(APPLICATION_SSB_DATASET_STRUCTURE_JSON);
        SUPPORTED_TYPES.add(APPLICATION_SSB_DATASET_DATA_JSON_V2);
    }

    public DatasetHttpMessageConverter(ObjectMapper objectMapper) {
        super(objectMapper);
        dataConverter = new DataHttpConverter(objectMapper);
        structureConverter = new DataStructureHttpConverter(objectMapper);
    }

    private DatasetHttpMessageConverter() {
        this(new ObjectMapper());
    }

    /*
     * @see
     */
    @Override
    public boolean canRead(Type type, Class<?> contextClass, MediaType mediaType) {
        return canRead(TypeToken.of(type), mediaType);
    }

    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return canRead(TypeToken.of(clazz), mediaType);
    }

    private boolean canRead(TypeToken<?> token, MediaType mediaType) {
        return canRead(mediaType) && (token.isSupertypeOf(STREAM_TYPE_TOKEN) ||
                token.isSupertypeOf(DataStructure.class) ||
                token.isSupertypeOf(Dataset.class));
    }

    @Override
    public boolean canWrite(Type type, Class<?> clazz, MediaType mediaType) {
        return canWrite(clazz, mediaType);
    }

    @Override
    public boolean canWrite(Class<?> clazz, MediaType mediaType) {
        TypeToken<?> token = TypeToken.of(clazz);
        return canWrite(mediaType) && (token.isSubtypeOf(STREAM_TYPE_TOKEN) ||
                token.isSubtypeOf(DataStructure.class) ||
                token.isSubtypeOf(Dataset.class));
    }




    @Override
    public List<MediaType> getSupportedMediaTypes() {
        return SUPPORTED_TYPES;
    }

    @Override
    public Object read(Type type, Class<?> contextClass, HttpInputMessage inputMessage)
            throws IOException, HttpMessageNotReadableException {
        return readInternal(TypeToken.of(type), inputMessage);
    }

    @Override
    protected Object readInternal(Class<?> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        return readInternal(TypeToken.of(clazz), inputMessage);
    }

    protected Object readInternal(TypeToken<?> token, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {

        ObjectMapper mapper = getObjectMapper();
        JsonParser parser = mapper.getFactory().createParser(inputMessage.getBody());

        // Advance to { "": [ <--
        checkToken(parser, parser.nextValue(), JsonToken.START_OBJECT);
        checkToken(parser, parser.nextValue(), JsonToken.START_ARRAY);

        // Expect { "structure": [
        checkCurrentName(parser, "structure");
        DataStructure structure = structureConverter.readWithParser(parser);

        if (token.isSupertypeOf(DataStructure.class))
            return structure;

        // Advance to { "structure": {}, "" : [ <--
        checkToken(parser, parser.nextValue(), JsonToken.START_ARRAY);

        // Expect { "data": [
        checkCurrentName(parser, "data");

        // Advance to { "structure": {}, "" : [[ <--
        parser.nextValue();

        MappingIterator<List<Object>> data = mapper.readerFor(LIST_TYPE_REFERENCE)
                .readValues(parser);

        Stream<List<Object>> rawStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        data, Spliterator.IMMUTABLE
                ), false
        );

        Stream<DataPoint> convertedStream = rawStream.map(pointWrappers -> {
            return pointWrappers.stream()
                    .map(VTLObject::of)
                    .collect(Collectors.toList()
                    );
        }).map(DataPoint::create);

        if (token.isSupertypeOf(STREAM_TYPE_TOKEN))
            return convertedStream.onClose(() -> {
                        try {
                            parser.close();
                        } catch (IOException e) {
                            throw new RuntimeException(format("could not close parser %s", parser), e);
                        }
                    }
            );

        List<DataPoint> dataPoints = convertedStream.collect(Collectors.toList());

        return new Dataset() {
            @Override
            public DataStructure getDataStructure() {
                return structure;
            }

            @Override
            public Stream<DataPoint> getData() {
                return dataPoints.stream();
            }

            @Override
            public Optional<Map<String, Integer>> getDistinctValuesCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getSize() {
                return Optional.empty();
            }
        };
    }

    @Override
    protected void writeInternal(Object o, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        writeInternal(o, o.getClass(), outputMessage);
    }

    @Override
    protected void writeInternal(Object object, Type type, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {

        if (!(object instanceof Dataset))
            throw new IllegalArgumentException(format("Got wrong object type %s", object.getClass()));

        Dataset dataset = (Dataset) object;

        // Make sure that Component order is always the same.
        DataStructure structure = dataset.getDataStructure();
        DataStructure sortedStructure = sortDataStructure(structure);

        ObjectMapper mapper = getObjectMapper();

        // Delegate to DataStructureHttpConverter if requested type matches.
        MediaType contentType = outputMessage.getHeaders().getContentType();
        if (structureConverter.canWrite(DataStructure.class, contentType)) {
            structureConverter.writeInternal(sortedStructure, outputMessage);
            return;
        }

        // Delegate to DataHttpConverter if requested type matches.
        if (dataConverter.canWrite(STREAM_TYPE_TOKEN.getType(), null, contentType)) {
            try (Stream<DataPoint> stream = dataset.getData()) {
                dataConverter.write(stream, contentType, outputMessage);
                return;
            }
        }

        try (JsonGenerator generator = mapper.getFactory().createGenerator(outputMessage.getBody())) {
            generator.writeStartObject();

            generator.writeArrayFieldStart(STRUCTURE_FIELD_NAME);
            structureConverter.writeWithParser(sortedStructure, generator);
            generator.writeEndArray();

            generator.writeArrayFieldStart(DATA_FIELD_NAME);

            try (Stream<DataPoint> data = dataset.getData()) {
                Iterator<DataPoint> it = data.iterator();
                while (it.hasNext()) {
                    Map<Component, VTLObject> next = structure.asMap(it.next());
                    generator.writeStartArray(next.size());
                    for (Component component : sortedStructure.values()) {
                        mapper.writeValue(generator, next.get(component).get());
                    }
                    generator.writeEndArray();
                }
            }

            generator.writeEndArray();
            generator.writeEndObject();


        }
    }

    /**
     * Sort the given DataStructure by role and then name.
     */
    static DataStructure sortDataStructure(DataStructure structure) {
        Set<Map.Entry<String, Component>> sortedEntrySet = Sets.newTreeSet(BY_ROLE.thenComparing(BY_NAME));
        sortedEntrySet.addAll(structure.entrySet());
        return DataStructure.builder().putAll(sortedEntrySet).build();
    }

    private static void checkArgument(JsonParser parser, boolean check, String message) throws JsonMappingException {
        if (!check) {
            throw JsonMappingException.from(parser, message);
        }
    }

    private static void checkArgument(JsonParser parser, boolean check, String message, Object... arg) throws JsonMappingException {
        if (!check) {
            throw JsonMappingException.from(parser, format(message, arg));
        }
    }

    private static void checkToken(JsonParser parser, JsonToken token, JsonToken expToken) throws JsonMappingException {
        checkArgument(
                parser, token == expToken,
                "Unexpected token (%s), expected %s",
                token, expToken
        );
    }

    private static void checkCurrentName(JsonParser parser, String prop) throws IOException {
        checkArgument(parser, prop.equals(parser.getCurrentName()), format("Unrecognized field \"%s\", expected \"%s\"",
                parser.getCurrentName(), prop
        ));
    }
}
