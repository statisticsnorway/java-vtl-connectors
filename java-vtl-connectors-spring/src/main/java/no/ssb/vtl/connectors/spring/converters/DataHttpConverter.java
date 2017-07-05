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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.google.common.reflect.TypeToken;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.VTLObject;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * A converter that can read and write data point streams.
 * <p>
 * It supports reading Stream<DataPoint> from:
 * <ul>
 * <li>application/ssb.dataset.data+json ; [version=2]"</li>
 * </ul>
 * <p>
 * And writes:
 * <ul>
 * <li>application/ssb.dataset.data+json;version=2</li>
 * </ul>
 */
public class DataHttpConverter extends AbstractGenericHttpMessageConverter<Stream<DataPoint>> {

    // Example:
    // http://www.mocky.io/v2/594a3fb4100000ae021aa3c2
    public static final String APPLICATION_SSB_DATASET_DATA_JSON_V2_VALUE = "application/ssb.dataset.data+json;version=2";
    public static final MediaType APPLICATION_SSB_DATASET_DATA_JSON_V2 = MediaType.parseMediaType(APPLICATION_SSB_DATASET_DATA_JSON_V2_VALUE);


    // @formatter:off
    private static final TypeToken<Stream<DataPoint>> SUPPORTED_TYPE = new TypeToken<Stream<DataPoint>>() {};
    private static final TypeReference<List<VTLObjectWrapper>> LIST_TYPE_REFERENCE = new TypeReference<List<VTLObjectWrapper>>() {};
    // @formatter:on

    private final ObjectMapper mapper;
    private final boolean requireVersion;
    private JsonFactory factory;
    private ObjectWriter rowWriter;

    public DataHttpConverter(ObjectMapper mapper) {
        this(mapper, true);
    }

    public DataHttpConverter(ObjectMapper mapper, boolean requireVersion) {
        super(
                APPLICATION_SSB_DATASET_DATA_JSON_V2
        );
        this.mapper = checkNotNull(mapper);
        this.rowWriter = this.mapper.writerFor(LIST_TYPE_REFERENCE);
        this.factory = this.mapper.getFactory();
        this.requireVersion = requireVersion;

        // TODO: Initialize readers.
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        throw new UnsupportedOperationException();
    }

    /**
     * @see DataHttpConverter#canWrite(TypeToken, MediaType)
     */
    @Override
    public boolean canWrite(Class<?> clazz, MediaType mediaType) {
        return canWrite(TypeToken.of(clazz), mediaType);
    }

    /**
     * @see DataHttpConverter#canWrite(TypeToken, MediaType)
     */
    @Override
    public boolean canWrite(Type type, Class<?> clazz, MediaType mediaType) {
        return canWrite(TypeToken.of(type), mediaType);
    }

    /**
     * @see #canWrite(Type, Class, MediaType)
     * @see #canWrite(Class, MediaType)
     */
    private boolean canWrite(TypeToken<?> token, MediaType mediaType) {
        return token.isSubtypeOf(SUPPORTED_TYPE) && canRead(mediaType);
    }

    /**
     * @see DataHttpConverter#canRead(TypeToken, MediaType)
     */
    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return canRead(TypeToken.of(clazz), mediaType);
    }

    /**
     * @see DataHttpConverter#canRead(TypeToken, MediaType)
     */
    @Override
    public boolean canRead(Type type, Class<?> contextClass, MediaType mediaType) {
        // TODO: Maybe use context?
        return canRead(TypeToken.of(type), mediaType);
    }

    @Override
    protected boolean canRead(MediaType mediaType) {
        if (super.canRead(mediaType)) {
            if (!requireVersion)
                return true;
            else
                return mediaType != null && "2".equals(mediaType.getParameter("version"));
        } else {
            return false;
        }
    }

    /**
     * @see #canRead(Type, Class, MediaType)
     * @see #canRead(Class, MediaType)
     */
    private boolean canRead(TypeToken<?> token, MediaType mediaType) {
        return token.isSupertypeOf(SUPPORTED_TYPE) && canRead(mediaType);
    }

    @Override
    public Stream<DataPoint> read(Type type, Class<?> contextClass, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        return readInternal(null, inputMessage);
    }

    Stream<DataPoint> readWithParser(JsonParser parser) throws IOException {

        // TODO: Manually deserialize (use mapper.readerFor() and types).

        MappingIterator<List<VTLObjectWrapper>> data = mapper.readerFor(LIST_TYPE_REFERENCE)
                .readValues(parser);

        Stream<List<VTLObjectWrapper>> rawStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        data, Spliterator.IMMUTABLE
                ), false
        );

        return rawStream.map(pointWrappers -> {
            return pointWrappers.stream()
                    .map(this::toVTLObject)
                    .collect(toList()
                    );
        }).map(DataPoint::create).onClose(() -> {
            try {
                parser.close();
            } catch (IOException e) {
                throw new RuntimeException(format("failed to close %s", parser), e);
            }
        });
    }

    @Override
    protected Stream<DataPoint> readInternal(Class<? extends Stream<DataPoint>> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        JsonParser parser = factory.createParser(inputMessage.getBody());
        parser.nextValue();
        parser.nextValue();
        return readWithParser(parser);
    }


    @Override
    protected void writeInternal(Stream<DataPoint> stream, Type type, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        try (
                JsonGenerator generator = factory.createGenerator(outputMessage.getBody());
                SequenceWriter sequenceWriter = rowWriter.writeValues(generator);
                Stream<DataPoint> closedStream = stream
        ) {

            generator.writeStartArray();
            for (DataPoint point : (Iterable<DataPoint>) closedStream::iterator) {
                List<VTLObjectWrapper> wrapped = point.stream()
                        .map(this::fromVTLObject)
                        .collect(toList());
                sequenceWriter.write(wrapped);
            }
            generator.writeEndArray();
        }
    }

    private VTLObjectWrapper fromVTLObject(VTLObject object) {
        // TODO: Reuse same object to improve perf.
        VTLObjectWrapper wrapper = new VTLObjectWrapper();
        if (object == null || object.get() == null)
            return null;

        Class<?> type = object.get().getClass();
        wrapper.setType(RoleMapping.fromType(type));
        wrapper.setVal(object.get());

        return wrapper;
    }

    private VTLObject toVTLObject(VTLObjectWrapper VTLObjectWrapper) {
        if (VTLObjectWrapper == null || VTLObjectWrapper.val == null)
            return VTLObject.NULL;
        return VTLObject.of(
                mapper.convertValue(VTLObjectWrapper.val, VTLObjectWrapper.type.getType())
        );
    }

    private static class VTLObjectWrapper {
        private RoleMapping type;
        private Object val;

        public RoleMapping getType() {
            return type;
        }

        public void setType(RoleMapping type) {
            this.type = type;
        }

        public Object getVal() {
            return val;
        }

        public void setVal(Object val) {
            this.val = val;
        }
    }

}
