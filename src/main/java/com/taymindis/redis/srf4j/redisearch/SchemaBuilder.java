package com.taymindis.redis.srf4j.redisearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SchemaBuilder {

    /**
     * SCHEMA {identifier} [AS {attribute}]
     * [TEXT [NOSTEM] [WEIGHT {weight}] [PHONETIC {matcher}] | NUMERIC | GEO | TAG [SEPARATOR {sep}]
     * [CASESENSITIVE] [SORTABLE [UNF]] [NOINDEX]]
     */

    private final List<CharSequence> fields;

    public SchemaBuilder() {
        fields = new ArrayList<>();
    }

    public void addField(String fieldName, CommandParam fieldType, CommandParam... args) {
        fields.add(fieldName);
        fields.add(fieldType);
        fields.addAll(Arrays.asList(args));
    }

    public List<CharSequence> build() {
        return fields;
    }
}
