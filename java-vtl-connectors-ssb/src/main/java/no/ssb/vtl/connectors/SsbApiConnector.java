package no.ssb.vtl.connectors;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import no.ssb.jsonstat.v2.DatasetBuildable;
import no.ssb.vtl.model.Dataset;
import org.kohsuke.MetaInfServices;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;

import static java.lang.String.*;

/**
 * A VTL connector that gets data from api.ssb.no.
 */
@MetaInfServices
public class SsbApiConnector extends JsonStatConnector {
    
    /*
        The list of available datasets:
        http://data.ssb.no/api/v0/dataset/list.json?lang=en

        Example dataset:
        http://data.ssb.no/api/v0/dataset/1106.json?lang=en

     */
    
    public SsbApiConnector(ObjectMapper mapper) {
        super(mapper);
    }
    
    
    public boolean canHandle(String identifier) {
        return identifier.startsWith("http://data.ssb.no/api/v0/dataset/");
    }

    public Dataset getDataset(String identifier) throws ConnectorException {
        
        ParameterizedTypeReference<Map<String, DatasetBuildable>> ref = new ParameterizedTypeReference<Map<String, DatasetBuildable>>() {
            // Just a reference.
        };

        try {

            if (identifier.startsWith("http://data.ssb.no/api/v0/dataset/")) {
                identifier = identifier.replace("http://data.ssb.no/api/v0/dataset/", "");
            }
            //http://data.ssb.no/api/v0/dataset/1106.json?lang=en;
            ResponseEntity<Map<String, DatasetBuildable>> exchange = getRestTemplate().exchange(
                    "http://data.ssb.no/api/v0/dataset/{id}.json?lang=en",
                    HttpMethod.GET,
                    null, ref, identifier);

            if (!exchange.getBody().values().iterator().hasNext()) {
                throw new NotFoundException(format("empty dataset returned for the identifier %s", identifier));
            }
    
            return buildDataset(exchange);

        } catch (RestClientException rce) {
            throw new ConnectorException(
                    format("error when accessing the dataset with id %s", identifier),
                    rce
            );
        }
    }
    
}
