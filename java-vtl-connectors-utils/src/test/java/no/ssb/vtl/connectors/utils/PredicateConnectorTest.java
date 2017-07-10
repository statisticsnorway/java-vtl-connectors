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
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PredicateConnectorTest {

    private Connector alwaysTrueConnector;

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    @Before
    public void setUp() throws Exception {
        alwaysTrueConnector = mock(Connector.class);
        when(alwaysTrueConnector.canHandle(anyString())).thenReturn(true);
    }

    @Test
    public void testPredicate() throws Exception {

        PredicateConnector connector = PredicateConnector.create(alwaysTrueConnector, "true"::equals);

        softly.assertThat(alwaysTrueConnector.canHandle("false")).isTrue();
        softly.assertThat(alwaysTrueConnector.canHandle("true")).isTrue();
        softly.assertThat(connector.canHandle("false")).isFalse();
        softly.assertThat(connector.canHandle("true")).isTrue();

    }

    @Test
    public void testPredicates() throws Exception {

        PredicateConnector connector = PredicateConnector.create(
                alwaysTrueConnector,
                s -> s.startsWith("start"),
                s -> s.endsWith("end")
        );

        softly.assertThat(alwaysTrueConnector.canHandle("end")).isTrue();
        softly.assertThat(alwaysTrueConnector.canHandle("start")).isTrue();

        softly.assertThat(connector.canHandle("end start")).isFalse();
        softly.assertThat(connector.canHandle("start end")).isTrue();

    }

    @Test
    public void testPredicatesThrow() throws Exception {

        PredicateConnector connector = PredicateConnector.create(
                alwaysTrueConnector,
                true,
                Lists.newArrayList(
                        s -> s.startsWith("start"),
                        s -> s.endsWith("end")
                )
        );

        softly.assertThat(alwaysTrueConnector.canHandle("end")).isTrue();
        softly.assertThat(alwaysTrueConnector.canHandle("start")).isTrue();

        softly.assertThatThrownBy(() -> connector.canHandle("end start"));
        softly.assertThat(connector.canHandle("start end")).isTrue();

    }
}
