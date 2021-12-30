package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.intf.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

public class LettuFacadeImpl implements RedisFacade {

    private final RedisClient core;
    private final RedisURI redisURI;

    public LettuFacadeImpl(RedisURI redisURI) {
        this.redisURI = redisURI;
        this.core = RedisClient.create(redisURI);
    }

    @Override
    public Object getCore() {
        return core;
    }


    public RedisURI getRedisURI() {
        return redisURI;
    }

    @Override
    public Session createSession() {
        return new LettuSessionImpl(core.connect());
    }

    @Override
    public Session createSlaveSession() {
        throw new UnsupportedOperationException("Create Slave session is not support");
    }

    @Override
    public <K, V> PubSubSession<K, V> createPubSubSession(PubSubEvent<K, V> pubSubEvent) {
        return new LettuPubSubSessionImpl(core.connectPubSub(), pubSubEvent);
    }

    @Override
    public <V> ListSession<V> useListSession(String key) {
        return new LettuListSessionImpl(key, core.connect());
    }


    @Override
    public void close() {
        core.shutdown();
    }
}
