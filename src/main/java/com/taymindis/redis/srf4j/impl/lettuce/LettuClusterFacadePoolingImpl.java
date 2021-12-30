package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.intf.ListSession;
import com.taymindis.redis.srf4j.intf.Session;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class LettuClusterFacadePoolingImpl extends LettuClusterFacadeImpl {

    private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;

    public LettuClusterFacadePoolingImpl(RedisURI redisURI) {
        super(redisURI);
        pool = ConnectionPoolSupport
                .createGenericObjectPool(() -> ((RedisClusterClient)getCore()).connect(StringCodec.UTF8), new GenericObjectPoolConfig<>()); }

    @Override
    public Session createSession() {
        try {
            return new LettuClusterSessionImpl(pool.borrowObject());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return super.createSession();
    }

    @Override
    public <V> ListSession<V> useListSession(String key) {
        try {
            return new LettuClusterListSessionImpl(key, pool.borrowObject());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public void close() {
        try {
            pool.close();
        } finally {
        }
        super.close();
    }
}
