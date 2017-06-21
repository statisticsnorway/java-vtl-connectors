package no.ssb.vtl.tools.sandbox.connector.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.tools.sandbox.connector.spring.converters.DataHttpConverter;
import no.ssb.vtl.tools.sandbox.connector.spring.converters.DataStructureHttpConverter;
import no.ssb.vtl.tools.sandbox.connector.spring.nonclosing.NonClosingClientHttpResponse;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.AbstractClientHttpRequest;
import org.springframework.http.client.AbstractClientHttpRequestFactoryWrapper;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsAsyncClientHttpRequestFactory;
import org.springframework.http.client.Netty4ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by hadrien on 15/06/2017.
 */
public class RestTemplateConnectorTest {

    private RestTemplate template;
    private RestTemplateConnector restTemplateConnector;

    @Before
    public void setUp() throws Exception {
        template = new RestTemplate();

        SimpleClientHttpRequestFactory simpleClientHttpRequestFactory = getSimpleClientHttpRequestFactory();
        ClientHttpRequestFactory nonClosing = new AbstractClientHttpRequestFactoryWrapper(simpleClientHttpRequestFactory) {
            // TODO: Look at the buffering

            @Override
            protected ClientHttpRequest createRequest(URI uri, HttpMethod httpMethod, ClientHttpRequestFactory requestFactory) throws IOException {

                ClientHttpRequest originalRequest = requestFactory.createRequest(uri, httpMethod);

                return new AbstractClientHttpRequest() {

                    @Override
                    protected OutputStream getBodyInternal(HttpHeaders headers) throws IOException {
                        return originalRequest.getBody();
                    }

                    @Override
                    protected ClientHttpResponse executeInternal(HttpHeaders headers) throws IOException {

                        originalRequest.getHeaders().setAll(headers.toSingleValueMap());
                        ClientHttpResponse originalResponse = originalRequest.execute();

                        return new NonClosingClientHttpResponse() {

                            @Override
                            protected ClientHttpResponse delegate() {
                                return originalResponse;
                            }
                        };

                    }

                    @Override
                    public HttpMethod getMethod() {
                        return originalRequest.getMethod();
                    }

                    @Override
                    public URI getURI() {
                        return originalRequest.getURI();
                    }
                };
            }
        };

        template.setInterceptors(Lists.newArrayList(new AuthorizationTokenInterceptor()));

        List<HttpMessageConverter<?>> converters = Lists.newArrayList();
        converters.add(new DataStructureHttpConverter(new ObjectMapper()));
        converters.add(new DataHttpConverter(new ObjectMapper()));

        template.setMessageConverters(converters);

        template.setRequestFactory(nonClosing);


        restTemplateConnector = new RestTemplateConnector(template);
    }

    private Netty4ClientHttpRequestFactory getNetty4ClientHttpRequestFactory() {
        return new Netty4ClientHttpRequestFactory();
    }

    private SimpleClientHttpRequestFactory getSimpleClientHttpRequestFactory() {
        SimpleClientHttpRequestFactory simpleFactory = new SimpleClientHttpRequestFactory();
        simpleFactory.setBufferRequestBody(false);
        simpleFactory.setOutputStreaming(true);
        return simpleFactory;
    }

    private HttpComponentsAsyncClientHttpRequestFactory getHttpComponentsAsyncClientHttpRequestFactory() {
        HttpComponentsAsyncClientHttpRequestFactory httpComponentAsync = new HttpComponentsAsyncClientHttpRequestFactory();
        httpComponentAsync.setBufferRequestBody(false);
        return httpComponentAsync;
    }

    @Test
    public void getGet() throws Exception {
        Dataset dataset = restTemplateConnector.getDataset("http://al-kostra-app-test:7090/api/v2/data/EOy-Ul8CSpSxNDOl34ywtQ");

        //Dataset dataset = restTemplateConnector.getDataset("http://www.mocky.io/v2/5942eeae1200001610ddc64a");
        //dataset.getDataStructure();
        try (Stream<DataPoint> data = dataset.getData()) {
            data.forEach(vtlObjects -> {
                System.out.print(vtlObjects);
            });
        }
    }

    static final class AuthorizationTokenInterceptor implements ClientHttpRequestInterceptor {
        private final String TOKEN = "bearer " +
                "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9" +
                "." +
                "eyJ1cG4iOiJ1c2VyMiIsIkUtcG9zdCI6InVzZXIyQHNzYi5ubyIsInVzZXJfbmFtZSI6InVzZXIyIiwi" +
                "c2NvcGUiOlsicmVhZCIsIndyaXRlIl0sImlzcyI6Imh0dHA6Ly9hbC1rb3N0cmEtYXBwLXV0di5zc2Iu" +
                "bm86NzIwMCIsImV4cCI6MTgxMjI2NjgzNSwiYXV0aG9yaXRpZXMiOlsiUk9MRV9VU0VSIl0sImp0aSI6" +
                "IjUwYjdjZTA5LTRmYTctNDdmNS1hODBkLThiYjdjYjRmNDE3YyIsImNsaWVudF9pZCI6ImNvbW1vbmd1" +
                "aSIsIk5hdm4iOiJVc2VyMiwgVXNlcjIiLCJHcnVwcGVyIjpbIktvbXBpcyBFaWVuZG9tbWVyIl19" +
                "." +
                "g6NGZo7znjTKxGWXjUN272mfMnfjJPeBhSqQUCRcULgKQfXiaz0h8xG3higHQ3qhLPVW_5m0Ri94Fvv1" +
                "UMQPwZwt3vK74yYvtQjNd36odKGWVjurKXQabNTyGJag9EObsU2qVKFhtjBzU65yPv_c5CsBkQorfZy1" +
                "-niO5r2M8DwImgI3HnO2ECKeFs8RjnpWiL8jWjqu1cTMcXNRUfkxH0NjAmQsvZPqF_GoEYqbGrwTCK3i" +
                "cuCFbxosOLJ1qW_ryuGQQ0PVkyuLoFv8Z8cV-htLD473B8hoG49gOyLeCSueDGRjdtxOB0YJwR7xNJLp" +
                "JeVkxWCKc_W1LB_XkyMg7Q";

        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
            request.getHeaders().set("Authorization", TOKEN);
            return execution.execute(request, body);
        }
    }

}
