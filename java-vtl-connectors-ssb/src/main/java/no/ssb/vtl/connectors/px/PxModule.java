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

import com.fasterxml.jackson.databind.module.SimpleModule;
import no.ssb.vtl.model.DataStructure;

/**
 * A Jackson module that support deserialization of the px-web datastructure.
 */
public class PxModule extends SimpleModule {

    public PxModule() {
        super("SSB structure deserializer");
        addDeserializer(DataStructure.class, new DataStructureDeserializer());
    }
}
