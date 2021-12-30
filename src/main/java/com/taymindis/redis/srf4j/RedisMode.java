package com.taymindis.redis.srf4j;

public enum RedisMode {
    CLUSTER,
    SENTINEL,
    SINGLE,

    // TODO currently using ASYNC, reason using REACTIVE?
    REACTIVE,
    ASYNC

}
