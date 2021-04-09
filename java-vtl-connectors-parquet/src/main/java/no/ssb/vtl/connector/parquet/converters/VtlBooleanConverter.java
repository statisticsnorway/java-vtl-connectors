package no.ssb.vtl.connector.parquet.converters;

import no.ssb.vtl.model.VTLBoolean;

public class VtlBooleanConverter extends VtlObjectConverter {

    public VtlBooleanConverter(DataPointConverter dataPointConverter, int pos) {
        super(dataPointConverter, pos);
    }

    @Override
    public void addBoolean(boolean value) {
        writeValue(VTLBoolean.of(value));
    }
}
