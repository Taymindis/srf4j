package com.taymindis.redis.srf4j.redisearch;

import java.util.Collections;
import java.util.List;


public class SearchCommandBuilder extends CommandBuilder {

    private CommandTypeExt commandTypeExt = CommandTypeExt.FT_SEARCH;
    boolean hasQuery = false;

    public SearchCommandBuilder(CharSequence idxName) {
        values.add(idxName);
    }

    public String getIdxName() {
        return values.get(0).toString();
    }


    public CommandTypeExt getSr4JCommandTypeExt() {
        return commandTypeExt;
    }


    public SearchCommandBuilder query(CharSequence ...query) {
        if(hasQuery) {
            throw new IllegalStateException("Query has presented of this builder");
        }
        Collections.addAll(values, query);
        hasQuery = true;
        return this;
    }

    public SearchCommandBuilder query(List<CharSequence> query) {
        if(hasQuery) {
            throw new IllegalStateException("Query has presented of this builder");
        }
        values.addAll(query);
        hasQuery = true;
        return this;
    }

}
