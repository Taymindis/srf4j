package com.taymindis.redis.srf4j.redisearch;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CustomCommandBuilder extends CommandBuilder {

    private final ProtocolKeyword commandTypeExt;
    private final List<CharSequence> values = new ArrayList<>();

    public CustomCommandBuilder(ProtocolKeyword commandTypeExt) {
        this.commandTypeExt = commandTypeExt;
    }

    public ProtocolKeyword getSr4JCommandTypeExt() {
        return commandTypeExt;
    }

    public CustomCommandBuilder addIdxOrQuery(CharSequence ...idxOrQueries) {
        Collections.addAll(values, idxOrQueries);
        return this;
    }

    public List<CharSequence> build() {
        return values;
    }
}
