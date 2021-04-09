package no.ssb.vtl.connector.parquet.converters;

import no.ssb.vtl.model.VTLObject;
import org.apache.parquet.io.api.PrimitiveConverter;

public class VtlObjectConverter extends PrimitiveConverter {

    final DataPointConverter dpc;
    final int pos;

    public VtlObjectConverter(DataPointConverter dataPointConverter, int pos) {
        this.dpc = dataPointConverter;
        this.pos = pos;
    }

    void writeValue(VTLObject value) {
        dpc.writeValue(value, pos);
    }
}
