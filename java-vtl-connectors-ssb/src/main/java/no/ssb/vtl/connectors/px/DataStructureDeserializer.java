package no.ssb.vtl.connectors.px;

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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;

import java.io.IOException;

public final class DataStructureDeserializer extends StdDeserializer<DataStructure> {

    public static final String VARIABLES_FIELD_NAME = "variables";
    public static final String CODE_FIELD_NAME = "code";
    public static final String MEASURE_NAME = "ContentsCode";
    public static final String VALUES_FIELD_NAME = "values";

    public DataStructureDeserializer() {
        super(DataStructure.class);
    }

    @Override
    public DataStructure deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);

        JsonNode measures = null;
        JsonNode identifiers = node.get(VARIABLES_FIELD_NAME);
        DataStructure.Builder builder = DataStructure.builder();
        for (JsonNode variable : identifiers) {
            String name = variable.get(CODE_FIELD_NAME).asText();
            if (name.equals(MEASURE_NAME)) {
                measures = variable.get(VALUES_FIELD_NAME);
            } else {
                builder.put(name, Component.Role.IDENTIFIER, String.class);
            }
        }
        for (JsonNode measure : measures) {
            builder.put(measure.asText(), Component.Role.MEASURE, Double.class);
        }
        return builder.build();
    }
}
