package no.ssb.vtl.tools.sandbox.connector.spring.converters;

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
