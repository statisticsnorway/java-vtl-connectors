package no.ssb.vtl.connectors.spring;

/*-
 * ========================LICENSE_START=================================
 * Java VTL Spring connector
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

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A connector that relies on {@link RestTemplate}.
 * <p>
 * In order to allow streaming the requests needs to be started in another
 * thread because RestTemplate will close the connection.
 * <p>
 * This class solves this issue by executing the requests in a thread pools and
 * transfer the result to a wrapped stream.
 */
public class RestTemplateConnector implements Connector {

    private static final Logger log = LoggerFactory.getLogger(RestTemplateConnector.class);
    private static final ParameterizedTypeReference<Stream<DataPoint>> DATAPOINT_STREAM_TYPE;

    public static Integer DEFAULT_BUFFER_SIZE = 1000;

    //@formatter:off
    static {
        DATAPOINT_STREAM_TYPE = new ParameterizedTypeReference<Stream<DataPoint>>() {};
    }
    //@formatter:on

    private final AsyncTaskExecutor executorService;
    private final WrappedRestTemplate template;
    private final Integer bufferSize;

    public RestTemplateConnector(RestTemplate template, Executor executorService) {
        this(template, new TaskExecutorAdapter(checkNotNull(executorService)));
    }

    public RestTemplateConnector(RestTemplate template, AsyncTaskExecutor executorService) {
        this(template, executorService, null);
    }

    private RestTemplateConnector(RestTemplate template, AsyncTaskExecutor executorService, Integer bufferSize) {
        this.template = new WrappedRestTemplate(checkNotNull(template));
        this.executorService = checkNotNull(executorService);
        this.bufferSize = Optional.ofNullable(bufferSize).orElse(DEFAULT_BUFFER_SIZE);
    }

    private Stream<DataPoint> getData(URI uri) {

        // We wrap the blocking queue in a Spliterator and let another thread handle the
        // connection and deserialization.

        final BlockingQueue<DataPoint> queue = Queues.newArrayBlockingQueue(bufferSize);
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final Thread reader = Thread.currentThread();
        final CountDownLatch latch = new CountDownLatch(1);
        log.debug("opening stream for {} (queue {})", uri, queue.hashCode());
        Future<Void> task = executorService.submit(() -> {

            try {
                RequestCallback requestCallback = template.httpEntityCallback(null, DATAPOINT_STREAM_TYPE.getType());

                template.execute(uri, HttpMethod.GET, requestCallback, response -> {

                    latch.countDown();

                    ResponseExtractor<ResponseEntity<Stream<DataPoint>>> extractor;
                    extractor = template.responseEntityExtractor(DATAPOINT_STREAM_TYPE.getType());

                    ResponseEntity<Stream<DataPoint>> responseEntity = extractor.extractData(response);

                    try (Stream<DataPoint> stream = responseEntity.getBody()) {

                        Iterator<DataPoint> it = stream.iterator();
                        while (it.hasNext()) {
                            DataPoint e = it.next();
                            queue.put(e);
                        }

                        queue.put(BlockingQueueSpliterator.EOS);
                        log.debug("done streaming for {} (queue {})", uri, queue.hashCode());

                    } catch (InterruptedException e) {
                        log.debug("interrupted while pushing datapoints from {} (queue {})", uri, queue.hashCode());
                        reader.interrupt();
                    } catch (Exception e) {
                        log.debug("error while pushing datapoints from {} (queue {})", uri, queue.hashCode(), e);
                        exception.set(e);
                        reader.interrupt();
                    }
                    return null;
                });

            } catch (Exception e) {
                log.error("error while reading {} data (queue {})", uri, queue.hashCode(), e);
                exception.set(e);
                reader.interrupt();
            }
            return null;
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            // Keep the interrupt flag.
            Thread.currentThread().interrupt();
        }

        Spliterator<DataPoint> spliterator = new BlockingQueueSpliterator(queue, task, exception);
        Stream<DataPoint> stream = StreamSupport.stream(spliterator, false);

        return stream.onClose(() -> {
            log.debug("closing stream for {} (queue {})", uri, queue.hashCode());
            task.cancel(true);
        });
    }

    private DataStructure getStructure(URI uri) {
        ResponseEntity<DataStructure> structureEntity = template.getForEntity(uri, DataStructure.class);
        return structureEntity.getBody();
    }

    private boolean checkResourceExists(URI uri) {
        try {
            Set<HttpMethod> allowed = template.optionsForAllow(uri);
            if (allowed.contains(HttpMethod.HEAD)) {
                template.headForHeaders(uri);
            }
        } catch (HttpClientErrorException hcre) {
            if (HttpStatus.NOT_FOUND.equals(hcre.getStatusCode()))
                return false;
            throw hcre;
        }
        return true;
    }

    @Override
    public boolean canHandle(String identifier) {
        try {
            URI uri = new URI(identifier);
            return checkResourceExists(uri);
        } catch (URISyntaxException e) {
            log.warn("could not convert {} to URI", identifier, e);
        }
        return false;
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        return new RestTemplateDataset(URI.create(identifier));
    }

    @Override
    public Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Exposes methods of the RestTemplate.
     */
    public class WrappedRestTemplate extends RestTemplate {

        public WrappedRestTemplate(RestTemplate template) {

            // TODO(hk): Check if this has consequences.
            // setDefaultUriVariables();

            setErrorHandler(template.getErrorHandler());
            setInterceptors(template.getInterceptors());
            setMessageConverters(template.getMessageConverters());
            setRequestFactory(template.getRequestFactory());
            setUriTemplateHandler(getUriTemplateHandler());
        }

        @Override
        public <T> RequestCallback acceptHeaderRequestCallback(Class<T> responseType) {
            return super.acceptHeaderRequestCallback(responseType);
        }

        @Override
        protected <T> ResponseExtractor<ResponseEntity<T>> responseEntityExtractor(Type responseType) {
            return super.responseEntityExtractor(responseType);
        }

        @Override
        protected <T> RequestCallback httpEntityCallback(Object requestBody, Type responseType) {
            return super.httpEntityCallback(requestBody, responseType);
        }
    }

    @VisibleForTesting
    static UriComponentsBuilder createOrderUri(UriComponentsBuilder uri, Order order, DataStructure structure) {

        // Add the sort parameter.
        List<String> sortParams = StreamUtils.aggregate(order.entrySet().stream(), (e1, e2) -> e1.getValue().equals(e2.getValue()))
                .map(entries -> {
                    Order.Direction direction = entries.get(0).getValue();
                    return entries.stream()
                            .map(Map.Entry::getKey)
                            .map(structure::getName)
                            .collect(Collectors.joining(
                                    ",", "", ",".concat(direction.toString())
                            ));
                })
                .collect(Collectors.toList());

        return uri.cloneBuilder().replaceQueryParam("sort", (Object[]) sortParams.toArray(new Object[]{}));
    }

    private class RestTemplateDataset implements Dataset {

        private final URI uri;
        private DataStructure structure;

        private RestTemplateDataset(URI uri) {
            this.uri = uri;
        }

        @Override
        public String toString() {
            URI uriWithoutQuery;
            try {
                uriWithoutQuery = new URI(uri.getScheme(), uri.getHost(), uri.getPath(), null);
            } catch (URISyntaxException e) {
                uriWithoutQuery = uri;
            }
            return toStringHelper(this)
                    .add("uri", uriWithoutQuery)
                    .toString();
        }

        @Override
        public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUri(uri);
            if (!orders.isEmpty())
                uriBuilder = createOrderUri(uriBuilder, orders, getDataStructure());

            return Optional.of(RestTemplateConnector.this.getData(uriBuilder.build().toUri()));
        }

        @Override
        public Optional<Stream<DataPoint>> getData(Order order) {
            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUri(uri);
            UriComponentsBuilder uriWithOrder = createOrderUri(uriBuilder, order, getDataStructure());
            return Optional.of(RestTemplateConnector.this.getData(uriWithOrder.build().toUri()));
        }

        @Override
        public Stream<DataPoint> getData() {
            // Always return a new stream.
            return RestTemplateConnector.this.getData(uri);
        }

        @Override
        public Optional<Map<String, Integer>> getDistinctValuesCount() {
            return Optional.empty();
        }

        @Override
        public Optional<Long> getSize() {
            return Optional.empty();
        }

        @Override
        public DataStructure getDataStructure() {
            if (structure != null)
                return structure;

            structure = RestTemplateConnector.this.getStructure(uri);
            return structure;
        }
    }


}
