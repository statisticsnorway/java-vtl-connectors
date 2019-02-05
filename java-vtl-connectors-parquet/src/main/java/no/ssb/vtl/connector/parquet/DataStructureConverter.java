package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.parquet.schema.OriginalType.TIME_MILLIS;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Converts a DataStructure to MessageType
 */
public class DataStructureConverter implements Function<DataStructure, MessageType> {

    private static PrimitiveType getPrimitiveType(Map.Entry<String, Component> entry) {

        Component component = entry.getValue();
        Types.PrimitiveBuilder<PrimitiveType> builder;
        if (String.class.isAssignableFrom(component.getType())) {
            builder = getBuilder(component, BINARY).as(UTF8);
        } else if (Long.class.isAssignableFrom(component.getType())) {
            builder = (getBuilder(component, INT64));
        } else if (Double.class.isAssignableFrom(component.getType())) {
            builder = (getBuilder(component, DOUBLE));
        } else if (Boolean.class.isAssignableFrom(component.getType())) {
            builder = (getBuilder(component, BOOLEAN));
        } else if (Instant.class.isAssignableFrom(component.getType())) {
            // TODO: Maybe builder = (getBuilder(component, INT64)).as(TIME_MICROS);
            builder = (getBuilder(component, INT64)).as(TIME_MILLIS);
        } else {
            throw new IllegalArgumentException("Unsupported component type " + entry.getValue());
        }
        String name = entry.getKey();
        return builder.named(name);
    }

    private static Types.PrimitiveBuilder<PrimitiveType> getBuilder(Component component, PrimitiveType.PrimitiveTypeName binary) {
        return component.isIdentifier() ? Types.required(binary).columnOrder(ColumnOrder.typeDefined()) : Types.optional(binary);
    }

    @Override
    public MessageType apply(DataStructure structure) {
        List<Type> types = new ArrayList<>();
        for (Map.Entry<String, Component> entry : structure.entrySet()) {
            types.add(getPrimitiveType(entry));
        }
        return new MessageType(ParquetConnector.DATAPOINT_TYPE_NAME, types);
    }
}
