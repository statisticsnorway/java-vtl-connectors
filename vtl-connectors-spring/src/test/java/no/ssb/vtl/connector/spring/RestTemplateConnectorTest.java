package no.ssb.vtl.connector.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import org.junit.Test;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import no.ssb.vtl.connector.spring.converters.DataHttpConverter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Created by hadrien on 13/06/17.
 */
public class RestTemplateConnectorTest {

    @Test
    public void getData() throws Exception {

        // Setup the factory.

        SimpleClientHttpRequestFactory schrf = new SimpleClientHttpRequestFactory();
        schrf.setBufferRequestBody(false);
        schrf.setTaskExecutor(new SimpleAsyncTaskExecutor());

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        RestTemplate template = new RestTemplate(schrf);

        template.getInterceptors().add(
                new AuthorizationTokenInterceptor()
        );

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        template.getMessageConverters().add(
                0, new DataHttpConverter(mapper)
        );

        RestTemplateConnector restTemplateConnector = new RestTemplateConnector(
                template,
                executorService
        );

        Dataset dataset = restTemplateConnector.getDataset("http://www.mocky.io/v2/594a48ee10000081031aa3fc");
        Stream<DataPoint> data = dataset.getData();
        data.forEach(System.out::println);

    }

}
