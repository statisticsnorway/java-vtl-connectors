package no.ssb.vtl.connectors.utils;

import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.model.Dataset;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class RegexConnector extends ForwardingConnector {

    private final Connector delegate;
    private final Pattern pattern;
    private final String replaceAll;

    private RegexConnector(Connector connector, Pattern pattern, String replaceAll) {
        this.delegate = checkNotNull(connector);
        this.pattern = checkNotNull(pattern);
        this.replaceAll = checkNotNull(replaceAll);
    }

    public static final RegexConnector create(Connector connector, Pattern pattern, String replaceAll) {
        return new RegexConnector(connector, pattern, replaceAll);
    }

    String transformId(String identifier) {
        Matcher matcher = pattern.matcher(identifier);
        if (!matcher.matches())
            return identifier;

        return matcher.replaceAll(replaceAll);
    }

    @Override
    public boolean canHandle(String identifier) {
        return delegate().canHandle(transformId(identifier));
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        return delegate().getDataset(transformId(identifier));
    }

    @Override
    public Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException {
        return delegate().putDataset(transformId(identifier), dataset);
    }

    @Override
    protected Connector delegate() {
        return delegate;
    }

}
