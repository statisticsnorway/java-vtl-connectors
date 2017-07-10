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

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

/**
 * Created by hadrien on 21/06/2017.
 */
final class AuthorizationTokenInterceptor implements ClientHttpRequestInterceptor {
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
