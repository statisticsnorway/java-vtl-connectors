package no.ssb.vtl.connector.parquet.converters;

import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.VTLObject;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataPointConverter extends GroupConverter {

    private final VtlObjectConverter[] converters;
    private VTLObject[] buffer;

    public DataPointConverter(DataStructure structure) {
        this.converters = createConverters(structure);
        this.buffer = new VTLObject[converters.length];
    }

    private VtlObjectConverter[] createConverters(DataStructure structure) {
        List<VtlObjectConverter> converters = new ArrayList<>();
        for (String name : structure.keySet()) {
            Class<?> type = structure.get(name).getType();
            if (String.class.isAssignableFrom(type)) {
                converters.add(new VtlStringConverter(this, converters.size()));
            } else if (Boolean.class.isAssignableFrom(type)) {
                converters.add(new VtlStringConverter(this, converters.size()));
            } else if (Instant.class.isAssignableFrom(type)) {
                converters.add(new VtlDateConverter(this, converters.size()));
            } else if (Long.class.isAssignableFrom(type)) {
                converters.add(new VtlIntegerConverter(this, converters.size()));
            } else if (Double.class.isAssignableFrom(type)) {
                converters.add(new VtlFloatConverter(this, converters.size()));
            } else {
                throw new IllegalArgumentException("Unsupported component type " + structure.get(name));
            }
        }
        return converters.toArray(new VtlObjectConverter[]{});
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    public DataPoint getCurrent() {
        return DataPoint.create(buffer);
    }

    public void writeValue(VTLObject object, int pos) {
        buffer[pos] = object;
    }

    @Override
    public void start() {
        Arrays.fill(buffer, VTLObject.NULL);
    }

    @Override
    public void end() {
    }
}
