package no.ssb.vtl.connectors.utils;

/*-
 * ========================LICENSE_START=================================
 * Java VTL Utility connectors
 * %%
 * Copyright (C) 2017 Statistics Norway and contributors
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */



import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import no.ssb.vtl.connector.Connector;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
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
