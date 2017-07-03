package no.ssb.vtl.connectors.spring.converters;

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

import com.google.common.reflect.TypeToken;

import java.time.Instant;

/**
 * A role to class mapping.
 */
public enum RoleMapping {

    STRING(String.class),
    INTEGER(Long.class),
    NUMERIC(Double.class),
    DATE(Instant.class),
    BOOLEAN(Boolean.class),

    // Kept for compatibility.
    FLOAT(Double.class),
    BOOL(Boolean.class);

    private final Class<?> clazz;

    RoleMapping(Class<?> clazz) {
        this.clazz = clazz;
    }

    public static RoleMapping fromType(Class<?> type) {
        TypeToken<?> token = TypeToken.of(type);

        if (token.isSubtypeOf(String.class))
            return STRING;

        if (token.isSubtypeOf(Long.class))
            return INTEGER;

        if (token.isSubtypeOf(Integer.class))
            return INTEGER;

        if (token.isSubtypeOf(Double.class))
            return NUMERIC;

        if (token.isSubtypeOf(Float.class))
            return NUMERIC;

        if (token.isSubtypeOf(Instant.class))
            return DATE;

        if (token.isSubtypeOf(Boolean.class))
            return BOOLEAN;

        throw new UnsupportedOperationException("could not convert type" + type);
    }

    public Class<?> getType() {
        return this.clazz;
    }
}
