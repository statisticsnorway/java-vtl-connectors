package no.ssb.vtl.connector.parquet.converters;

import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;

public class DataPointMaterializer extends RecordMaterializer<DataPoint> {

    private final DataPointConverter groupConverter;

    public DataPointMaterializer(DataStructure structure) {
        groupConverter = new DataPointConverter(structure);
    }

    @Override
    public DataPoint getCurrentRecord() {
        return groupConverter.getCurrent();
    }

    @Override
    public GroupConverter getRootConverter() {
        return groupConverter;
    }
}
