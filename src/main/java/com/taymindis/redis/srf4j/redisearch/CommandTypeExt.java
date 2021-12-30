package com.taymindis.redis.srf4j.redisearch;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum CommandTypeExt implements ProtocolKeyword {
    FT_INFO,
    FT_SEARCH,
    FT_AGGREGATE,
    FT_CREATE,
    FT_DROPINDEX,
    FT__LIST
    ;
    public final byte[] bytes;

    CommandTypeExt() {
        this.bytes = this.name().replace("FT_", "FT.").getBytes(StandardCharsets.US_ASCII);
    }

    public byte[] getBytes() {
        return this.bytes;
    }
}
