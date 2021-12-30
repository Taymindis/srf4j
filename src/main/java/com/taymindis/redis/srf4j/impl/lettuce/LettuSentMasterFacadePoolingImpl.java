package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.intf.ListSession;
import com.taymindis.redis.srf4j.intf.Session;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.List;

public class LettuSentMasterFacadePoolingImpl extends LettuSentMasterFacadeImpl {

    private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;;

    public LettuSentMasterFacadePoolingImpl(List<String> sentinelsUri, String masterId, String userName, CharSequence password) {
        super(sentinelsUri, masterId, userName, password);
        pool = ConnectionPoolSupport
                .createGenericObjectPool(() -> ((RedisClient)getCore()).connect(), new GenericObjectPoolConfig<>());
    }

    @Override
    public Session createSession() {
        try {
            return new LettuSentMasterSessionImpl(pool.borrowObject());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return super.createSession();
    }

    @Override
    public <V> ListSession<V> useListSession(String key) {
        try {
            return new LettuListSessionImpl(key, pool.borrowObject());
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
