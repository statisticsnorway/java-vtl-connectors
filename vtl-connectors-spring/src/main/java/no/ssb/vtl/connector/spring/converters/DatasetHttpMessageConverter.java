package no.ssb.vtl.connector.spring.converters;

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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A converter that support the following conversions
 *
 * Read:
 * application/ssb.dataset+json;version=2 -> DataStructure
 * application/ssb.dataset+json;version=2 -> Stream<DataPoint>
 *
 * Write:
 *
 * Dataset -> application/ssb.dataset+json;version=2
 * Dataset -> application/ssb.dataset.data+json;version=2
 * Dataset -> application/ssb.dataset.structure+json;version=2
 *
 */
class DatasetHttpMessageConverter extends MappingJackson2HttpMessageConverter {

    public static final String APPLICATION_DATASET_JSON_VALUE = "application/ssb.dataset+json;version=2";

    public static final MediaType APPLICATION_DATASET_JSON = MediaType.parseMediaType(APPLICATION_DATASET_JSON_VALUE);

    private final DataHttpConverter dataConverter;
    private final DataStructureHttpConverter structureConverter;

    private static final TypeReference<List<Object>> LIST_TYPE_REFERENCE = new TypeReference<List<Object>>() {
    };

    @VisibleForTesting
    static final List<MediaType> SUPPORTED_TYPES;

    static {
        SUPPORTED_TYPES = new ArrayList<>();
        SUPPORTED_TYPES.add(APPLICATION_DATASET_JSON);
    }

    public DatasetHttpMessageConverter(ObjectMapper objectMapper) {
        super(objectMapper);
        dataConverter = new DataHttpConverter(objectMapper);
        structureConverter = new DataStructureHttpConverter(objectMapper);
    }

    private DatasetHttpMessageConverter() {
        this(new ObjectMapper());
    }

    @Override
    public Object read(Type type, Class<?> contextClass, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        JavaType javaType = getJavaType(type, contextClass);
        return readInternal(javaType.getRawClass(), inputMessage);
    }

    @Override
    public boolean canRead(Type type, Class<?> contextClass, MediaType mediaType) {
        return canRead(getJavaType(type, contextClass).getRawClass(), mediaType);
    }

    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return canRead(mediaType) && isClassSupported(clazz);
    }

    private boolean isClassSupported(Class<?> clazz) {
        return clazz.isAssignableFrom(DataStructure.class) ||
                clazz.isAssignableFrom(Stream.class) ||
                clazz.isAssignableFrom(Dataset.class);
    }

    @Override
    public boolean canWrite(Class<?> clazz, MediaType mediaType) {
        return canWrite(mediaType) && isClassSupported(clazz);
    }

    @Override
    public List<MediaType> getSupportedMediaTypes() {
        return SUPPORTED_TYPES;
    }

    @Override
    protected Dataset readInternal(Class<?> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        ObjectMapper mapper = getObjectMapper();

        JsonParser parser = mapper.getFactory().createParser(inputMessage.getBody());

        // Advance to { "": [ <--
        checkToken(parser, parser.nextValue(), JsonToken.START_OBJECT);
        checkToken(parser, parser.nextValue(), JsonToken.START_ARRAY);

        // Expect { "structure": [
        checkCurrentName(parser, "structure");

        DataStructure structure = structureConverter.readWithParser(parser);

        // Advance to { "structure": {}, "" : [ <--
        checkToken(parser, parser.nextValue(), JsonToken.START_ARRAY);

        // Expect { "data": [
        checkCurrentName(parser, "data");

        parser.nextValue();

        MappingIterator<List<Object>> data = mapper.readerFor(LIST_TYPE_REFERENCE)
                .readValues(parser);

        Stream<List<Object>> rawStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        data, Spliterator.IMMUTABLE
                ), false
        );

        List<DataPoint> dataPoints = rawStream.map(pointWrappers -> {
            return pointWrappers.stream()
                    .map(VTLObject::of)
                    .collect(Collectors.toList()
                    );
        }).map(DataPoint::create).collect(Collectors.toList());

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
    protected void writeInternal(Object object, Type type, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        super.writeInternal(object, type, outputMessage);
    }

    private static void checkArgument(JsonParser parser, boolean check, String message) throws JsonMappingException {
        if (!check) {
            throw JsonMappingException.from(parser, message);
        }
    }

    private static void checkArgument(JsonParser parser, boolean check, String message, Object... arg) throws JsonMappingException {
        if (!check) {
            throw JsonMappingException.from(parser, String.format(message, arg));
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
        checkArgument(parser, prop.equals(parser.getCurrentName()), String.format("Unrecognized field \"%s\", expected \"%s\"",
                parser.getCurrentName(), prop
        ));
    }
}
