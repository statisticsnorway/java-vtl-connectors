package no.ssb.vtl.connectors.utils.jackson;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;

import java.io.IOException;

public class DataStructureDeserializer extends StdDeserializer<DataStructure> {

    private static final String NAME_PROPERTY_NAME = "name";
    private static final String ROLE_PROPERTY_NAME = "role";
    private static final String TYPE_PROPERTY_NAME = "type";

    public DataStructureDeserializer() {
        super(DataStructure.class);
    }

    @Override
    public DataStructure deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

        DataStructure.Builder structureBuilder = DataStructure.builder();
        if (p.currentToken() != JsonToken.START_ARRAY) {
            throw new JsonParseException(p, "expected an array");
        }
        while (p.nextValue() != JsonToken.END_ARRAY) {
            if (p.currentToken() != JsonToken.START_OBJECT) {
                throw new JsonParseException(p, "expected an object");
            }
            String name = null;
            Component.Role role = null;
            Class<?> type = null;
            while (p.nextValue() != JsonToken.END_OBJECT) {
                String currentName = p.getCurrentName();
                if (NAME_PROPERTY_NAME.equals(currentName)) {
                    name = p.getValueAsString();
                    continue;
                }
                if (ROLE_PROPERTY_NAME.equals(currentName)) {
                    role = Component.Role.valueOf(p.getValueAsString());
                    continue;
                }
                if (TYPE_PROPERTY_NAME.equals(currentName)) {
                    type = TypeMapping.valueOf(p.getValueAsString()).getType();
                    continue;
                }
            }
            if (name == null) {
                throw new JsonParseException(p, "missing name");
            }
            if (role == null) {
                throw new JsonParseException(p, "missing role");
            }
            if (type == null) {
                throw new JsonParseException(p, "missing type");
            }
            structureBuilder.put(name, role, type);
        }
        return structureBuilder.build();
    }
}
