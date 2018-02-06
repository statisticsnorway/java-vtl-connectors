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

import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.model.Dataset;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class RegexConnector extends ForwardingConnector {

    private final Connector delegate;
    private final Pattern pattern;
    private final String replaceAll;

    private RegexConnector(Connector connector, Pattern pattern, String replaceAll) {
        this.delegate = checkNotNull(connector);
        this.pattern = checkNotNull(pattern);
        this.replaceAll = checkNotNull(replaceAll);
    }

    public static final RegexConnector create(Connector connector, Pattern pattern, String replaceAll) {
        return new RegexConnector(connector, pattern, replaceAll);
    }

    String transformId(String identifier) {
        Matcher matcher = pattern.matcher(identifier);
        if (!matcher.matches())
            return identifier;

        return matcher.replaceAll(replaceAll);
    }

    @Override
    public boolean canHandle(String identifier) {
        return delegate().canHandle(transformId(identifier));
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        return delegate().getDataset(transformId(identifier));
    }

    @Override
    public Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException {
        return delegate().putDataset(transformId(identifier), dataset);
    }

    @Override
    protected Connector delegate() {
        return delegate;
    }

}
