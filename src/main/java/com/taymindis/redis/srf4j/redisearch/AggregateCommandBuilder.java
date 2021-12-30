package com.taymindis.redis.srf4j.redisearch;


public class AggregateCommandBuilder extends SearchCommandBuilder {

    private CommandTypeExt commandTypeExt = CommandTypeExt.FT_AGGREGATE;

    public AggregateCommandBuilder(CharSequence idxName) {
        super(idxName);
    }

    @Override
    public CommandTypeExt getSr4JCommandTypeExt() {
        return commandTypeExt;
    }



}
