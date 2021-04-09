package no.ssb.vtl.connectors.utils.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;

import java.io.IOException;
import java.util.Map;

public class DataStructureSerializer extends StdSerializer<DataStructure> {

    public DataStructureSerializer() {
        super(DataStructure.class);
    }

    @Override
    public void serialize(DataStructure structure, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartArray(structure.size());
        for (Map.Entry<String, Component> variable : structure.entrySet()) {
            Component component = variable.getValue();
            gen.writeStartObject();
            gen.writeStringField("name", variable.getKey());
            gen.writeStringField("role", component.getRole().name());
            gen.writeStringField("type", TypeMapping.fromType(component.getType()).name());
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }
}
