package no.ssb.vtl.tools.sandbox.connector.util;

import com.google.common.collect.ForwardingObject;
import no.ssb.vtl.connector.Connector;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A {@link Connector} which forwards all its method calls to another connector.
 */
public abstract class ForwardingDataset extends ForwardingObject implements Dataset {

    @Override
    protected abstract Dataset delegate();

    @Override
    public Stream<DataPoint> getData() {
        return this.delegate().getData();
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return this.delegate().getDistinctValuesCount();
    }

    @Override
    public Optional<Long> getSize() {
        return this.delegate().getSize();
    }

    @Override
    public DataStructure getDataStructure() {
        return this.delegate().getDataStructure();
    }

}
