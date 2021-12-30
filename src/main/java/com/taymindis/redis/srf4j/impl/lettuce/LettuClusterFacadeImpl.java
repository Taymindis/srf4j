package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.intf.*;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;

public class LettuClusterFacadeImpl implements RedisFacade {

    private final RedisClusterClient core;
    private final RedisURI redisURI;

    public LettuClusterFacadeImpl(RedisURI redisURI) {
        this.redisURI = redisURI;
        this.core = RedisClusterClient.create(redisURI);
    }

    @Override
    public Session createSession() {
        return new LettuClusterSessionImpl(core.connect(StringCodec.UTF8));
    }

    @Override
    public Session createSlaveSession() {
        throw new UnsupportedOperationException("Create Slave session is not support");
    }


    @Override
    public <K,V> PubSubSession<K,V> createPubSubSession(PubSubEvent<K, V> pubSubEvent) {
        return new LettuClusterPubSubSessionImpl(core.connectPubSub(), pubSubEvent);
    }

    @Override
    public <V> ListSession<V> useListSession(String key) {
        return new LettuClusterListSessionImpl(key, core.connect());
    }

    @Override
    public void close() {
        core.shutdown();
    }


    @Override
    public Object getCore() {
        return core;
    }

}
