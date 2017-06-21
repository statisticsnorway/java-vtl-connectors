package no.ssb.vtl.tools.sandbox.connector.util;

import com.google.common.collect.ForwardingObject;
import no.ssb.vtl.connector.Connector;
import no.ssb.vtl.connector.ConnectorException;
import no.ssb.vtl.model.Dataset;

/**
 * A {@link Connector} which forwards all its method calls to another connector.
 */
public abstract class ForwardingConnector extends ForwardingObject implements Connector {

    @Override
    protected abstract Connector delegate();

    @Override
    public boolean canHandle(String identifier) {
        return delegate().canHandle(identifier);
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        return delegate().getDataset(identifier);
    }

    @Override
    public Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException {
        return putDataset(identifier, dataset);
    }
}
