//package com.taymindis.redis.srf4j.impl.lettuce;
//
//import com.redislabs.lettusearch.RediSearchClient;
//import com.redislabs.lettusearch.StatefulRediSearchConnection;
//import com.taymindis.redis.srf4j.intf.SearchFacade;
//import com.taymindis.redis.srf4j.intf.SearchSession;
//import io.lettuce.core.RedisURI;
//
//import java.time.Duration;
//import java.util.List;
//
//public class LettuSentMasterSearchFacadeImpl implements SearchFacade {
//
//    private final RediSearchClient core;
//    private final RedisURI redisURI;
//    private static final String DEFAULT_CLUSTERED_ID = "mymaster";
//
//
//
//    public LettuSentMasterSearchFacadeImpl(RedisURI redisURI) {
////        List<RedisURI> sentinels = redisURI.getSentinels();
////        if (sentinels.size() < 3) {
////            throw new IllegalStateException("Min clustered of 3 is required to achieve fail over operation");
////        }
////// Setup Sentinels master discovery
////        RedisURI sentinel = sentinels.get(0);
////
////        RedisURI.Builder builder = RedisURI.Builder
////                .sentinel(sentinel.getHost(),
////                        sentinel.getPort(),
////                        redisURI.getSentinelMasterId());
////
////        for (int i = 1; i < sentinels.size(); i++) {
////            sentinel = sentinels.get(i);
////            builder.withSentinel(sentinel.getHost(),sentinel.getPort());
////        }
//
////        RedisURI masterSentinelsClusteredUri = builder.build();
//        this.core = RediSearchClient.create(redisURI);
//        this.redisURI = redisURI;
//    }
//
//    @Override
//    public SearchSession createSession() {
////        RedisSentinelAsyncConnection<String, String>  connection = client.connectSentinelAsync();
//        StatefulRediSearchConnection connection = core.<String, Object>connect();
//        return new LettuSearchSessionImpl(connection);
//    }
//
//    @Override
//    public Object getCore() {
//        return core;
//    }
//
//
//    @Override
//    public void close() {
//        core.shutdown();
////        core.shutdown(Duration.ofSeconds(10L), Duration.ofSeconds(30L));
//    }
//}
