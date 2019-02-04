package no.ssb.vtl.connector.parquet.converters;

import no.ssb.vtl.model.VTLFloat;

public class VtlFloatConverter extends VtlObjectConverter {
    public VtlFloatConverter(DataPointConverter dataPointConverter, int pos) {
        super(dataPointConverter, pos);
    }

    @Override
    public void addFloat(float value) {
        writeValue(VTLFloat.of(value));
    }
}
