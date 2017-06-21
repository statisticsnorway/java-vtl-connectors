package no.ssb.vtl.tools.sandbox.connector.spring;

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
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by hadrien on 15/06/2017.
 */
@Deprecated
public class RestTemplateConnector implements Connector {

    private static final Logger log = LoggerFactory.getLogger(RestTemplateConnector.class);

    private final RestTemplate template;
    private final ClientHttpRequestFactory factory;

    protected RestTemplateConnector(RestTemplate template) {
        this.template = checkNotNull(template);
        this.factory = template.getRequestFactory();
    }

    private Stream<DataPoint> getData(URI uri) {
//        ParameterizedTypeReference<Stream<DataPoint>> type = new ParameterizedTypeReference<Stream<DataPoint>>() {
//        };
//        ResponseEntity<Stream<DataPoint>> dataEntity = template.exchange(uri, HttpMethod.GET, null, type);

        ParameterizedTypeReference<DataPointStream> type = new ParameterizedTypeReference<DataPointStream>() {
        };

        ResponseEntity<DataPointStream> dataEntity = template.exchange(uri, HttpMethod.GET, null, type);

//        ResponseEntity<DataPointStream> dataEntity =  template.execute(uri, HttpMethod.GET, null, response -> {
//            new ResponseExtractor<>()
//        });


        return dataEntity.getBody();
    }

    private DataStructure getStructure(URI uri) {
        ResponseEntity<DataStructure> structureEntity = template.getForEntity(uri, DataStructure.class);
        return structureEntity.getBody();
    }

    @Override
    public boolean canHandle(String identifier) {
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

    private interface DataPointStream extends Stream<DataPoint> {
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
