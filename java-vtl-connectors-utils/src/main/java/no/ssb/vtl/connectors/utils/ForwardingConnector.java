package no.ssb.vtl.connectors.utils;

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

import com.google.common.collect.ForwardingObject;
import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.model.Dataset;

/**
 * A {@link Connector} which forwards all its method calls to another connector.
 */
public abstract class ForwardingConnector extends ForwardingObject implements Connector {

    @Override
    protected abstract Connector delegate();

    @Override
    public boolean canHandle(String identifier) {
        return delegate().canHandle(identifier);
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        return delegate().getDataset(identifier);
    }

    @Override
    public Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException {
        return delegate().putDataset(identifier, dataset);
    }
}
