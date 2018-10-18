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


import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link Connector} that saves data in a cache.
 */
public abstract class CachedConnector extends ForwardingConnector {

    private static final Logger log = LoggerFactory.getLogger(CachedConnector.class);

    private final Cache<String, CacheProxyDataset> datasetCache;
    private final Cache<String, ImmutableList<DataPoint>> sortableCache;
    private final Cache<SortedKey, ImmutableList<DataPoint>> sortedCache;

    private CachedConnector(CacheBuilder<Object, Object> cacheSpec) {
        checkNotNull(cacheSpec);
        this.datasetCache = cacheSpec.recordStats().build();
        this.sortableCache = cacheSpec.recordStats().build();
        this.sortedCache = cacheSpec.recordStats().build();
    }

    private CachedConnector() {
        this.datasetCache = CacheBuilder.newBuilder().recordStats().build();
        this.sortableCache = CacheBuilder.newBuilder().recordStats().build();
        this.sortedCache = CacheBuilder.newBuilder().recordStats().build();
    }

    public static CachedConnector create(Connector connector) {
        return new CachedConnector() {
            @Override
            protected Connector delegate() {
                return connector;
            }
        };
    }

    public static CachedConnector create(Connector connector, CacheBuilder<Object, Object> cacheSpec) {
        return new CachedConnector(cacheSpec) {
            @Override
            protected Connector delegate() {
                return connector;
            }
        };
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        try {
            return datasetCache.get(identifier, () -> {
                Dataset dataset = super.getDataset(identifier);
                return new CacheProxyDataset(identifier, sortableCache, sortedCache) {
                    @Override
                    protected Dataset delegate() {
                        return dataset;
                    }
                };
            });
        } catch (ExecutionException e) {
            throw new ConnectorException("cache error", e.getCause());
        }
    }

    /**
     * Key for sorted values.
     */
    private final static class SortedKey {
        private final String identifier;
        private final Order order;

        private SortedKey(String identifier, Order order) {
            this.identifier = checkNotNull(identifier);
            this.order = checkNotNull(order);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .addValue(identifier)
                    .add("order", order)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SortedKey sortedKey = (SortedKey) o;
            return Objects.equal(identifier, sortedKey.identifier) &&
                    Objects.equal(order, sortedKey.order);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(identifier, order);
        }
    }

    private static final class CacheSpliterator extends Spliterators.AbstractSpliterator<DataPoint> {

        private final Runnable callback;
        private final Spliterator<DataPoint> spliterator;
        private boolean hasMore = true;

        private CacheSpliterator(Runnable callback, Spliterator<DataPoint> spliterator) {
            super(spliterator.estimateSize(), spliterator.characteristics() /* TODO: check this */);
            this.spliterator = spliterator;
            this.callback = checkNotNull(callback);
        }

        @Override
        public boolean tryAdvance(Consumer<? super DataPoint> action) {
            if (!hasMore)
                return false;

            hasMore = spliterator.tryAdvance(action);
            if (!hasMore)
                callback.run();

            return true;
        }

        @Override
        public void forEachRemaining(Consumer<? super DataPoint> action) {
            spliterator.forEachRemaining(action);
            callback.run();
        }
    }

    protected static abstract class CacheProxyDataset extends ForwardingDataset {

        private final String identifier;
        private final Cache<String, ImmutableList<DataPoint>> sortableCache;
        private final Cache<SortedKey, ImmutableList<DataPoint>> sortedCache;

        protected CacheProxyDataset(String identifier, Cache<String, ImmutableList<DataPoint>> sortableCache, Cache<SortedKey, ImmutableList<DataPoint>> sortedCache) {
            this.identifier = checkNotNull(identifier);
            this.sortableCache = checkNotNull(sortableCache);
            this.sortedCache = checkNotNull(sortedCache);
        }


        @Override
        public Stream<DataPoint> getData() {
            ImmutableList<DataPoint> cachedData = sortableCache.getIfPresent(identifier);
            if (cachedData != null)
                return cachedData.stream().map(DataPoint::create);

            // Compute the data.
            ImmutableList.Builder<DataPoint> cacheDataBuilder = ImmutableList.builder();
            Stream<DataPoint> stream = delegate().getData().peek(dataPoint -> {
                cacheDataBuilder.add(DataPoint.create(dataPoint));
            });

            // Hook on the spliterator to know when the stream is finished.
            return StreamSupport.stream(new CacheSpliterator(() -> {
                sortableCache.put(identifier, cacheDataBuilder.build());
            }, stream.spliterator()), false).onClose(stream::close);
        }

        @Override
        public Optional<Stream<DataPoint>> getData(Order order) {
            SortedKey key = new SortedKey(identifier, order);
            ImmutableList<DataPoint> sortedCachedData = sortedCache.getIfPresent(key);
            if (sortedCachedData != null)
                return Optional.of(sortedCachedData.stream().map(DataPoint::create));

            // Compute the data.
            ImmutableList.Builder<DataPoint> cacheDataBuilder = ImmutableList.builder();
            Stream<DataPoint> sortedStream = delegate().getData(order).orElseThrow(() ->
                    new IllegalArgumentException("could not get sorted data from " + delegate())
            ).peek(dataPoint -> {
                cacheDataBuilder.add(DataPoint.create(dataPoint));
            });

            // Hook on the spliterator to know when the stream is finished.
            return Optional.of(StreamSupport.stream(new CacheSpliterator(() -> {
                sortedCache.put(key, cacheDataBuilder.build());
            }, sortedStream.spliterator()), false).onClose(sortedStream::close));
        }
    }
}
