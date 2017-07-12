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

import com.google.common.collect.Lists;
import no.ssb.vtl.connectors.Connector;

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A wrapper connector that checks identifiers against {@link Predicate}
 */
public abstract class PredicateConnector extends ForwardingConnector {

    private final Predicate<String> predicate;
    private final Boolean throwException;

    private PredicateConnector(Predicate<String> predicate, Boolean throwException) {
        this.predicate = predicate;
        this.throwException = throwException;
    }

    public static PredicateConnector create(Connector connector, Predicate<String> predicate) {
        return new PredicateConnector(predicate, false) {
            @Override
            protected Connector delegate() {
                return connector;
            }
        };
    }

    public static PredicateConnector create(Connector connector, Iterable<Predicate<String>> predicates) {
        return create(connector, false, predicates);
    }

    public static PredicateConnector create(Connector connector, Predicate<String>... predicates) {
        return create(connector, false, predicates);
    }

    public static PredicateConnector create(Connector connector, boolean raiseException, Predicate<String>... predicates) {
        return create(connector, raiseException, Lists.newArrayList(predicates));
    }

    public static PredicateConnector create(Connector connector, boolean raiseException, Iterable<Predicate<String>> predicates) {
        Predicate<String> composed = s -> true;
        for (Predicate<String> predicate : predicates)
            composed = composed.and(predicate);

        return new PredicateConnector(composed, raiseException) {
            @Override
            protected Connector delegate() {
                return connector;
            }
        };
    }

    public static PredicateConnector create(Connector connector, boolean raiseException, Predicate<String> predicate) {
        return new PredicateConnector(predicate, raiseException) {
            @Override
            protected Connector delegate() {
                return connector;
            }
        };
    }

    @Override
    public boolean canHandle(String identifier) {
        if (throwException) {
            checkArgument(
                    predicate.test(identifier),
                    "[%s] is not allowed by [%s]",
                    identifier, predicate
            );
        }
        return predicate.test(identifier) && super.canHandle(identifier);
    }
}
