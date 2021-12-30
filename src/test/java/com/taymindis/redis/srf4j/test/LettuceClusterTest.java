package com.taymindis.redis.srf4j.test;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;


public class LettuceClusterTest {
    @Test
    public void testRedisClusterReactivePipeline1() {
        try {
            RedisURI redisUri = RedisURI.Builder.redis("localhost", 6379)
//                    .withPassword("authentication")
                    .build();
            RedisClusterClient clusterClient = RedisClusterClient
                    .create(redisUri);

            final Set<Object> response = new HashSet<>();
            for (int i = 0; i < 2; i++) {

                try (StatefulRedisClusterConnection<String, String> connection = clusterClient.connect()) {
                    RedisAdvancedClusterReactiveCommands<String, String> reactiveCommands = connection.reactive();
                    reactiveCommands.setAutoFlushCommands(false);
                    try {
//
//                    reactiveCommands.set("PersonA", "QS@#").subscribe();
//                    reactiveCommands.expire("PersonA", 10).subscribe();

                        CountDownLatch a = new CountDownLatch(1);
                        //                                Integer.parseInt((String)serializable);
                        Flux.just(reactiveCommands.set("PersonA", "ASDASD"),
                                reactiveCommands.expire("PersonA", 10))
                                .flatMap(m -> m)
                                .doFinally(signalType -> {
                                    a.countDown();
                                })
                                .subscribe(new Consumer<Serializable>() {
                                    @Override
                                    public void accept(Serializable serializable) {
                                        response.add(serializable);
                                    }
                                });


//                   Flux.just("John")
//                        .flatMap(v -> {
//                            return reactiveCommands.set("PersonA", v);
//                         })
//                           .subscribe();

//                reactiveCommands.set("PersonA", "John");
//                            .next()
//                            .subscribe(o -> {
//                                System.out.println(o);
//                            });

                        reactiveCommands.flushCommands();
//                    LettuceFutures.awaitAll(Duration.ofSeconds(3600),
//                            response.toArray(new CompletableFuture[0]));

                        a.await();
                    } finally {
                        reactiveCommands.setAutoFlushCommands(true);
                    }

                    System.out.println(response);
                    Thread.sleep(5000);
                }


            }

        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testRedisReactiveClusterPipeline2() {
        try {
            RedisURI redisUri = RedisURI.Builder.redis("localhost")
//                    .withPassword("authentication")
                    .build();
            RedisClusterClient clusterClient = RedisClusterClient
                    .create(redisUri);
            try (StatefulRedisClusterConnection<String, String> connection = clusterClient.connect()) {
                RedisAdvancedClusterReactiveCommands<String, String> reactiveCommands = connection.reactive();

                reactiveCommands.setAutoFlushCommands(false);

                Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                        .flatMap(i -> Mono.just(reactiveCommands.set("key-" + i, "value-" + i * 5)).then(reactiveCommands.expire("key-" + i, 10)))
                        .subscribe();

// write all commands to the transport layer

                reactiveCommands.flushCommands();
//                disposables.dispose();
// synchronization example: Wait until all futures complete
//                boolean result = LettuceFutures.awaitAll(5, TimeUnit.SECONDS,
//                        futures.toArray(new RedisFuture<?>[0]));
                reactiveCommands.setAutoFlushCommands(true);
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

}
