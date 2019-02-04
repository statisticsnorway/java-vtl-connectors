package no.ssb.vtl.connector.parquet.converters;

import no.ssb.vtl.model.VTLInteger;

public class VtlIntegerConverter extends VtlObjectConverter {
    public VtlIntegerConverter(DataPointConverter dataPointConverter, int pos) {
        super(dataPointConverter, pos);
    }

    @Override
    public void addInt(int value) {
        writeValue(VTLInteger.of(value));
    }

    @Override
    public void addLong(long value) {
        writeValue(VTLInteger.of(value));
    }
}
