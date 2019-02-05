package no.ssb.vtl.connector.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.connectors.spring.converters.DataStructureHttpConverter;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.VTLObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.RecordConsumerLoggingWrapper;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ListIterator;

import static no.ssb.vtl.connector.parquet.ParquetConnector.STRUCTURE_META_NAME;

/**
 * Write {@link DataPoint}s to a Parquest file.
 */
public class DataPointWriter extends WriteSupport<DataPoint> {

    private final MessageType messageType;
    private final DataStructure structure;
    private RecordConsumer consumer;

    private DataPointWriter(DataStructure structure) {
        this.structure = structure;
        this.messageType = new DataStructureConverter().apply(structure);
    }


    public static DataPointWriter.Builder builder(OutputFile path, DataStructure structure) {
        return new Builder(path, structure);
    }

    @Override
    public WriteContext init(Configuration configuration) {
        // TODO: Extract jackson ser-deser from spring converter to utils.
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            new DataStructureHttpConverter(new ObjectMapper()).write(structure, null, new HttpOutputMessage() {
                @Override
                public OutputStream getBody() throws IOException {
                    return output;
                }

                @Override
                public HttpHeaders getHeaders() {
                    return new HttpHeaders();
                }
            });
            return new WriteContext(this.messageType, ImmutableMap.of(STRUCTURE_META_NAME, output.toString()));
        } catch (IOException e) {
            throw new RuntimeException("could not deserialize datastructure");
        }
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
            if (value.get() != null) {
                consumer.startField(messageType.getFieldName(pos), pos);
                PrimitiveType.PrimitiveTypeName typeName = messageType.getType(pos).asPrimitiveType().getPrimitiveTypeName();
                if (typeName.equals(PrimitiveType.PrimitiveTypeName.INT64)) {
                    consumer.addLong((Long) value.get());
                } else {
                    consumer.addBinary(Binary.fromCharSequence((CharSequence) value.get()));
                }
                consumer.endField(messageType.getFieldName(pos), pos);
            }
        }
        consumer.endMessage();
    }


    public static class Builder extends ParquetWriter.Builder<DataPoint, Builder> {

        private final DataStructure structure;

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
