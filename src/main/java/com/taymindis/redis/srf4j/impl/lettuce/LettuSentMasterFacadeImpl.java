package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.intf.*;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

import java.util.List;
import java.util.Objects;

public class LettuSentMasterFacadeImpl implements RedisFacade {

    private final RedisClient core;
    private final RedisURI redisURI;
    private final RedisCodec<String, String> UTF_8 = StringCodec.UTF8;

//    public LettuClusterFacadeImpl(RedisURI redisURI) {
//        this.redisURIs = List.of(redisURI);
//        this.core = RedisClusterClient.create(redisURI);
//    }

    public LettuSentMasterFacadeImpl(List<String> sentinelsUri, String masterId, String userName, CharSequence password) {
        if (sentinelsUri.size() < 3) {
            throw new IllegalStateException("Min of 3 clustered sentinels is required " +
                    "to achieve fail over methodology");
        }
        int i = 0;
        String[] hostPort = sentinelsUri.get(i).split(":");

        RedisURI.Builder builder = RedisURI.Builder.sentinel(hostPort[0],
                hostPort.length == 1 ? 26379 : Integer.parseInt(hostPort[1]), masterId);

        for (; i < sentinelsUri.size(); i++) {
            hostPort = sentinelsUri.get(i).split(":");
            builder = builder.withSentinel(hostPort[0],
                    hostPort.length == 1 ? 26379 : Integer.parseInt(hostPort[1]));

        }

        if(Objects.nonNull(userName) && Objects.nonNull(password) ) {
            builder.withAuthentication(userName, password);
        }

        this.redisURI = builder.build();
        core = RedisClient.create(this.redisURI);
    }

    @Override
    public Session createSession() {
        return new LettuSentMasterSessionImpl(core.connect());
    }

    @Override
    public Session createSlaveSession() {

//        StatefulRedisMasterSlaveConnection con = MasterSlave.connect(
//                core, UTF_8, redisURI);
//        con.setReadFrom(ReadFrom.SLAVE);
        StatefulRedisMasterReplicaConnection<String, String> con = MasterReplica.connect(
                core, UTF_8, redisURI);
        con.setReadFrom(ReadFrom.REPLICA);
//        cache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
//                .redisClient(client)
//                .connection(con)
//                .keyPrefix(new Random().nextInt() + "")
//                .buildCache();
//        cache.put("K1", "V1");
//        Thread.sleep(100);
        return new LettuSentMasterSessionImpl(con);

    }

    @Override
    public Object getCore() {
        return core;
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
