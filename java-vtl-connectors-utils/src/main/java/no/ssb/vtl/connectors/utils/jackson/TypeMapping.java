package no.ssb.vtl.connectors.utils.jackson;

import java.time.Instant;

public enum TypeMapping {

    STRING(String.class),
    INTEGER(Long.class),
    NUMERIC(Double.class),
    DATE(Instant.class),
    BOOLEAN(Boolean.class),
    NUMBER(Number.class),

    // Kept for compatibility.
    FLOAT(Double.class),
    BOOL(Boolean.class);

    private final Class<?> clazz;

    TypeMapping(Class<?> clazz) {
        this.clazz = clazz;
    }

    public static TypeMapping fromType(Class<?> type) {
        if (String.class.isAssignableFrom(type))
            return STRING;

        if (Long.class.isAssignableFrom(type))
            return INTEGER;

        if (Double.class.isAssignableFrom(type))
            return NUMERIC;

        if (Integer.class.isAssignableFrom(type))
            return INTEGER;

        if (Float.class.isAssignableFrom(type))
            return NUMERIC;

        if (Instant.class.isAssignableFrom(type))
            return DATE;

        if (Boolean.class.isAssignableFrom(type))
            return BOOLEAN;

        throw new UnsupportedOperationException("could not convert type" + type);
    }

    public Class<?> getType() {
        return this.clazz;
    }
}
