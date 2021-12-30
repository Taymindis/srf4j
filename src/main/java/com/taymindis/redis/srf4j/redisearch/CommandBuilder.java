package com.taymindis.redis.srf4j.redisearch;

import io.lettuce.core.protocol.ProtocolKeyword;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class CommandBuilder {
    final List<CharSequence> values = new ArrayList<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandBuilder.class.getName());


    public abstract ProtocolKeyword getSr4JCommandTypeExt();

    public CommandBuilder addOpt(CommandParam param, CharSequence ...args) {
        values.add(param);
        if (!Objects.isNull(args)) {
            Collections.addAll(values, args);
        }
        return this;
    }
    public CommandBuilder addOpt(CommandParam param, CommandParam param2, String args) {
        values.add(param);
        values.add(param2);
        if (!Objects.isNull(args)) {
            Collections.addAll(values, args.split(" "));
        }
        return this;
    }

    public CommandBuilder addOpt(CommandParam param, CommandParam param2,
                                 CommandParam param3, String args) {
        values.add(param);
        values.add(param2);
        values.add(param3);
        if (!Objects.isNull(args)) {
            Collections.addAll(values, args.split(" "));
        }
        return this;
    }

    /**
     Danger query is trying to query by ownself writing without following DOCS.
     This is only suggested when you have something advance but couldn't find from DOCS.
     * @param v
     * @return Self
     */
    public CommandBuilder dangerQuery(CharSequence ...v) {
        LOGGER.warn("Applying Danger Query ... ");
        Collections.addAll(values, v);
        return this;
    }

    public List<CharSequence> build() {
        return values;
    }
}
