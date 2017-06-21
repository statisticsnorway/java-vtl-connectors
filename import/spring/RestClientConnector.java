package no.ssb.vtl.tools.sandbox.connector.spring;

import com.google.common.collect.Queues;
import no.ssb.vtl.connector.Connector;
import no.ssb.vtl.connector.ConnectorException;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static no.ssb.vtl.tools.sandbox.connector.spring.BlockingQueueSpliterator.EOS;

/**
 * A connector that relies on {@link RestTemplate}.
 * <p>
 * In order to allow streaming the requests needs to be started in another
 * thread because RestTemplate will close the connection.
 * <p>
 * This class solves this issue by executing the requests in a thread pools and
 * transfer the result to a wrapped stream.
 */
public class RestClientConnector implements Connector {

    private static final Logger log = LoggerFactory.getLogger(RestClientConnector.class);

    public static final ParameterizedTypeReference<Stream<DataPoint>> DATAPOINT_STREAM_TYPE;

    //@formatter:off
    static {
        DATAPOINT_STREAM_TYPE = new ParameterizedTypeReference<Stream<DataPoint>>() {};
    }
    //@formatter:on

    private final ExecutorService executorService;
    private final WrappedRestTemplate template;

    public RestClientConnector(RestTemplate template, ExecutorService executorService) {
        this.template = new WrappedRestTemplate(checkNotNull(template));
        this.executorService = checkNotNull(executorService);
    }

    private Stream<DataPoint> getData(URI uri) {

        // Where the magic happens. We wrap the blocking queue in a Spliterator and let another thread handle the
        // connection and deserialization.

        final BlockingQueue<DataPoint> queue = Queues.newArrayBlockingQueue(100);
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final Thread reader = Thread.currentThread();

        Future<Void> task = executorService.submit(() -> {

            try {
                RequestCallback requestCallback = template.httpEntityCallback(null, DATAPOINT_STREAM_TYPE.getType());

                template.execute(uri, HttpMethod.GET, requestCallback, response -> {

                    ResponseExtractor<ResponseEntity<Stream<DataPoint>>> extractor;
                    extractor = template.responseEntityExtractor(DATAPOINT_STREAM_TYPE.getType());

                    ResponseEntity<Stream<DataPoint>> responseEntity = extractor.extractData(response);

                    try (Stream<DataPoint> stream = responseEntity.getBody()) {

                        Iterator<DataPoint> it = stream.iterator();
                        while (it.hasNext()) {
                            queue.put(it.next());
                        }
                        queue.put(EOS);

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.debug("interrupted while pushing datapoints to {}", queue);
                        reader.interrupt();
                    }
                    return null;
                });

            } catch (Exception e) {
                log.error("read error", e);
                exception.set(e);
                reader.interrupt();
            }
            return null;
        });

        Spliterator<DataPoint> spliterator = new BlockingQueueSpliterator(queue, task, exception);
        Stream<DataPoint> stream = StreamSupport.stream(spliterator, false);

        return stream.onClose(() -> task.cancel(true));
    }

    private DataStructure getStructure(URI uri) {
        ResponseEntity<DataStructure> structureEntity = template.getForEntity(uri, DataStructure.class);
        return structureEntity.getBody();
    }

    private boolean checkResourceExists(URI uri) {
        try {
            Set<HttpMethod> allowed = template.optionsForAllow(uri);
            if (allowed.contains(HttpMethod.HEAD)) {
                // TODO: Maybe check for content types?
                template.headForHeaders(uri);
                return true;
            }
        } catch (RestClientResponseException rcre) {
            return false;
        }
        return false;
    }

    @Override
    public boolean canHandle(String identifier) {
        try {
            URI uri = new URI(identifier);
            return checkResourceExists(uri);
        } catch (URISyntaxException e) {
            log.warn("Got invalid URI");
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

    private class RestTemplateDataset implements Dataset {

        private final URI uri;
        private DataStructure structure;

        private RestTemplateDataset(URI uri) {
            this.uri = uri;
        }

        @Override
        public Stream<DataPoint> getData() {
            // Always return a new stream.
            return RestClientConnector.this.getData(uri);
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

            structure = RestClientConnector.this.getStructure(uri);
            return structure;
        }
    }


}
