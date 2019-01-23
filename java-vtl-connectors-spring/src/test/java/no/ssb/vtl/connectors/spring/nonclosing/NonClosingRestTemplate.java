package no.ssb.vtl.connectors.spring.nonclosing;

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

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.AbstractClientHttpRequest;
import org.springframework.http.client.AbstractClientHttpRequestFactoryWrapper;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.Assert;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * POC.
 */
public class NonClosingRestTemplate extends RestTemplate {


    private final RestTemplate delegate;

    public NonClosingRestTemplate(RestTemplate delegate) {
        this.delegate = checkNotNull(delegate);

        // Replace the request factory by ours.
        ClientHttpRequestFactory requestFactory = delegate.getRequestFactory();

        this.delegate.setRequestFactory(getRequestFactory());

    }

    @Override
    public ClientHttpRequestFactory getRequestFactory() {
        return super.getRequestFactory();
    }

    private void createNonClosingFactory(ClientHttpRequestFactory factory) {

        ClientHttpRequestFactory nonClosing = new AbstractClientHttpRequestFactoryWrapper(factory) {
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
    }

    @Override
    protected <T> T doExecute(URI url, HttpMethod method, RequestCallback requestCallback, ResponseExtractor<T> responseExtractor) throws RestClientException {
        Assert.notNull(url, "'url' must not be null");
        Assert.notNull(method, "'method' must not be null");
        ClientHttpResponse response = null;
        try {
            ClientHttpRequest request = createRequest(url, method);
            if (requestCallback != null) {
                requestCallback.doWithRequest(request);
            }
            response = request.execute();
            handleResponse(url, method, response);
            if (responseExtractor != null) {
                return responseExtractor.extractData(response);
            } else {
                return null;
            }
        } catch (IOException ex) {
            String resource = url.toString();
            String query = url.getRawQuery();
            resource = (query != null ? resource.substring(0, resource.indexOf(query) - 1) : resource);
            throw new ResourceAccessException("I/O error on " + method.name() +
                    " request for \"" + resource + "\": " + ex.getMessage(), ex);
        } finally {
            if (response != null && !(response instanceof NonClosingClientHttpResponse)) {
                response.close();
            }
        }
    }
}
