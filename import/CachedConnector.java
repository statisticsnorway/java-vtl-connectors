package no.ssb.vtl.tools.sandbox.connector;

import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import no.ssb.vtl.connector.Connector;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.tools.sandbox.connector.util.ForwardingConnector;
import no.ssb.vtl.tools.sandbox.connector.util.ForwardingDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link Connector} wrapper that saves data in a cache.
 */
public abstract class CachedConnector extends ForwardingConnector {

    private static final Logger log = LoggerFactory.getLogger(CachedConnector.class);
    private final Cache<DatasetKey, Supplier<Stream<DataPoint>>> cache;

    private CachedConnector(Cache<DatasetKey, Supplier<Stream<DataPoint>>> cache) {
        this.cache = checkNotNull(cache);
    }

    abstract DatasetKey computeKey(
            String identifier,
            Order orders,
            Dataset.Filtering filtering,
            Set<String> components
    );

    private static class DatasetKey {

    }

    /**
     * A Dataset that checks if the structure is available.
     */
    private abstract class CachedDataset extends ForwardingDataset {

        private final String identifier = "";

        @Override
        public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
            try {
                DatasetKey key = computeKey(identifier, orders, filtering, components);
                Supplier<Stream<DataPoint>> ifPresent = cache.getIfPresent(key);
                if (ifPresent != null)
                    return Optional.of(ifPresent.get());
                else
                    return getData(orders, filtering, components);

            } catch (Exception e) {
                log.warn("could not compute cache key for {}", this);
                return delegate().getData(orders, filtering, components);
            }
        }
    }
}
