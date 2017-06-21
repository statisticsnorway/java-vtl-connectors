package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import static com.google.common.base.Preconditions.checkNotNull;

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

    public static final String MEDIA_TYPE_VALUE = "application/ssb.dataset.structure+json";
    public static final String MEDIA_TYPE_VALUE2 = "application/x-ssb.dataset.structure+json";

    public static final MediaType MEDIA_TYPE = MediaType.parseMediaType(MEDIA_TYPE_VALUE);

    private final ObjectMapper mapper;

    private final TypeReference<List<DataStructureWrapper>> TYPE_REFERENCE = new TypeReference<List<DataStructureWrapper>>() {
    };

    protected DataStructureHttpConverter(MediaType supportedMediaType, ObjectMapper mapper) {
        super(supportedMediaType);
        this.mapper = checkNotNull(mapper);
    }

    public DataStructureHttpConverter(ObjectMapper mapper) {
        this(MEDIA_TYPE, mapper);
    }

    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return clazz.isAssignableFrom(DataStructure.class) && canRead(mediaType);
    }

    @Override
    public boolean canWrite(Class<?> clazz, MediaType mediaType) {
        return DataStructure.class.isAssignableFrom(clazz) && canWrite(mediaType);
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
