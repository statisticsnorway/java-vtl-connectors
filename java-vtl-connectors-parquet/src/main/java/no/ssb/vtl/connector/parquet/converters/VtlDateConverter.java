package no.ssb.vtl.connector.parquet.converters;

import no.ssb.vtl.model.VTLDate;

import java.time.Instant;

public class VtlDateConverter extends VtlObjectConverter {

    public VtlDateConverter(DataPointConverter dataPointConverter, int pos) {
        super(dataPointConverter, pos);
    }

    @Override
    public void addLong(long value) {
        writeValue(VTLDate.of(Instant.ofEpochMilli(value)));
    }
}
