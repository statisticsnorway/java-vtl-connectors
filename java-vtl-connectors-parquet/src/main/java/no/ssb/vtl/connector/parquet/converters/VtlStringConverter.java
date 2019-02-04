package no.ssb.vtl.connector.parquet.converters;

import no.ssb.vtl.model.VTLString;
import org.apache.parquet.io.api.Binary;

public class VtlStringConverter extends VtlObjectConverter {

    public VtlStringConverter(DataPointConverter dataPointConverter, int pos) {
        super(dataPointConverter, pos);
    }

    @Override
    public void addBinary(Binary value) {
        writeValue(VTLString.of(value.toStringUsingUTF8()));
    }
}
