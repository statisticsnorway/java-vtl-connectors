package no.ssb.vtl.connector.parquet;

import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.VTLObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.RecordConsumerLoggingWrapper;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static org.apache.parquet.schema.OriginalType.TIME_MICROS;
import static org.apache.parquet.schema.OriginalType.TIME_MILLIS;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class DataPointWriter extends WriteSupport<DataPoint> {

    private final MessageType messageType;
    private RecordConsumer consumer;

    private DataPointWriter(DataStructure structure) {

        Types.required(BINARY).as(UTF8).named("test");
        List<Type> types = new ArrayList<>();
        for (Map.Entry<String, Component> entry : structure.entrySet()) {
            types.add(getPrimitiveType(entry));
        }
        this.messageType = new MessageType("datapoint", types);
    }

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

    public static DataPointWriter.Builder builder(OutputFile path, DataStructure structure) {
        return new Builder(path, structure);
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(this.messageType, Collections.emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.consumer = new RecordConsumerLoggingWrapper(recordConsumer);
    }

    @Override
    public void write(DataPoint o) {
        consumer.startMessage();
        ListIterator<VTLObject> it = o.listIterator();
        while (it.hasNext()) {
            int pos = it.nextIndex();
            VTLObject value = it.next();
            consumer.startField(messageType.getFieldName(pos), pos);
            consumer.addBinary(Binary.fromCharSequence((CharSequence) value.get()));
            consumer.endField(messageType.getFieldName(pos), pos);

        }
        consumer.endMessage();
    }

    public static class Builder extends ParquetWriter.Builder<DataPoint, Builder> {

        private final DataStructure structure;

        Builder(Path path, DataStructure structure) {
            super(path);
            this.structure = structure;
        }

        Builder(OutputFile path, DataStructure structure) {
            super(path);
            this.structure = structure;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<DataPoint> getWriteSupport(Configuration configuration) {
            return new DataPointWriter(structure);
        }
    }
}
