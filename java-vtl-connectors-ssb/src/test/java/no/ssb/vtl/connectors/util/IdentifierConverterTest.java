package no.ssb.vtl.connectors.util;

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


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class IdentifierConverterTest {
    
    @Test
    public void testConvertJson() throws Exception {
        InputStream queryStream = Resources.getResource(this.getClass(), "/query.json").openStream();
        String query = CharStreams.toString(new InputStreamReader(queryStream, Charsets.UTF_8));
        String urlParams = IdentifierConverter.toQueryString(query);
        assertThat(urlParams).isEqualTo("Region=all(*)&Eierskap=01+02-03+98&ContentsCode=Antall1&Tid=top(3)");
    }
    
    @Test
    public void toJson() throws Exception {
        JsonNode jsonNode = IdentifierConverter.toJson("Region=all(*)&Eierskap=01+02-03+98&ContentsCode=Antall1&Tid=top(3)");
        System.out.println(jsonNode);
        assertThat(jsonNode).isNotNull();
    
        InputStream queryStream = Resources.getResource(this.getClass(), "/query.json").openStream();
        String query = CharStreams.toString(new InputStreamReader(queryStream, Charsets.UTF_8));
        JsonNode jsonNode1 = new ObjectMapper().readTree(query);
        assertThat(jsonNode).isEqualTo(jsonNode1);
    }
}