package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.impl.lettuce.output.SearchListMapTypeOutput;
import com.taymindis.redis.srf4j.intf.SearchResult;
import com.taymindis.redis.srf4j.intf.Session;
import com.taymindis.redis.srf4j.redisearch.CommandBuilder;
import com.taymindis.redis.srf4j.redisearch.CommandTypeExt;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.protocol.CommandType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class LettuClusterSessionImpl extends BaseSessionImpl<String> implements Session {

    private final StatefulRedisClusterConnection<String, String> connection;

    public LettuClusterSessionImpl(StatefulRedisClusterConnection<String, String> connection) {
        super(connection);
        this.connection = connection;
    }

    @Override
    public Long insert(String key, Map<String, String> keyVal) {
        RedisAdvancedClusterCommands<String, String> sync = connection.sync();
        return sync.hset(key, keyVal);
    }

    @Override
    public List<Object> insert(String key, Map<String, String> keyVal, int expirable) throws ExecutionException, InterruptedException {
        List<Object> response = new ArrayList<>();
        RedisAdvancedClusterReactiveCommands<String, String> reactiveCommands = connection.reactive();

        reactiveCommands.setAutoFlushCommands(false);
        try {
            CountDownLatch c = new CountDownLatch(1);
            Flux<Mono<?>> monoFlux = Flux.just(reactiveCommands.hset(key, keyVal));

            if (expirable >= 0) {
                monoFlux.concatWithValues(reactiveCommands.expire(key, expirable));
            }

            monoFlux.flatMap(m -> m)
                    .doFinally(signalType -> {
                        c.countDown();
                    })
                    .subscribe(response::add);
            reactiveCommands.flushCommands();

            c.await();
        } finally {
            reactiveCommands.setAutoFlushCommands(true);
        }

        return response;
    }


    @Override
    public List<Object> batchExecTransaction(List<Map<String, String>> batchPayload) {
        return batchExecTransaction(batchPayload, -1);
    }

    @Override
    public List<Object> batchExecTransaction(List<Map<String, String>> batchPayload, int expirable) {
        return batchExecTransaction(batchPayload, expirable, null);
    }

    @Override
    public List<Object> batchExecTransaction(List<Map<String, String>> batchPayload, int expirable, Duration timeout) {
        throw new UnsupportedOperationException("Redis Cluster is not support transactional operation as there are multiple connection");
    }

    @Override
    public List<Object> batchExecPipeline(List<Map<String, String>> batchPayload) throws InterruptedException {
        return batchExecPipeline(batchPayload, -1);
    }

    @Override
    public List<Object> batchExecPipeline(List<Map<String, String>> batchPayload, int expirable) throws InterruptedException {
        return batchExecPipeline(batchPayload, expirable, null);
    }

    @Override
    public List<Object> batchExecPipeline(List<Map<String, String>> batchPayload, int expirable, Duration timeout) throws InterruptedException {
        List<RedisFuture<?>> response = new ArrayList<>();
        RedisAdvancedClusterAsyncCommands<String, String> async = connection.async();

        try {
            async.setAutoFlushCommands(false);
            for (Map<String, String> rowData : batchPayload) {
                if (rowData.get("__key") != null) {
                    String key = rowData.remove("__key");
                    response.add(async.hset(key, rowData));
                    if (expirable >= 0) {
                        response.add(async.expire(key, expirable));
                    }
                }
            }
            async.flushCommands();

            if (timeout != null) {
                LettuceFutures.awaitAll(timeout,
                        response.toArray(new RedisFuture[0]));
            } else {
                LettuceFutures.awaitAll(Duration.ofSeconds(3600), response.toArray(new RedisFuture[0]));
            }


            return response.stream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                return null;
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw e;
        } finally {
            async.setAutoFlushCommands(true);
        }


//        List<Mono<?>> monoList = new LinkedList<>();
//        List<Object> resp = new LinkedList<>();
//        RedisAdvancedClusterReactiveCommands<String, String> reactiveCommands = connection.reactive();
//        try {
//            reactiveCommands.setAutoFlushCommands(false);
//
//
//            for (Map<String, String> rowData : batchPayload) {
//                if (rowData.get("__key") != null) {
//                    String key = rowData.remove("__key");
//                    monoList.add(reactiveCommands.hset(key, rowData));
//                    if (expirable >= 0) {
//                        monoList.add(reactiveCommands.expire(key, expirable));
//                    }
//                }
//            }
//            CountDownLatch c = new CountDownLatch(1);
//            Flux.just(monoList.toArray(new Mono[0]))
//                    .flatMap(m -> m)
//                    .doFinally(signalType -> {
//                        c.countDown();
//                    })
//                    .subscribe(resp::add);
//
//            reactiveCommands.flushCommands();
//
//            c.await();
//
//            return resp;
//        } finally {
//            reactiveCommands.setAutoFlushCommands(true);
//        }
    }


    @Override
    public String getFieldValByKey(String key, String field) {
        return null;
    }

    @Override
    public Map<String, String> getAllByKey(String key) {
        return null;
    }

    @Override
    public boolean sqlLoad(String sqlQuery) {
        return false;
    }


    @Override
    public Long publish(String queueName, String message) {
        return null;
    }


    @Override
    public List<String> executeRawCommand(CommandTypeExt commandType, String query) throws RedisCommandExecutionException,ExecutionException, InterruptedException {
        return RedisFacadeOps.executeToRawStringList(commandType, connection, query);
    }

    @Override
    public List<String> executeRawCommand(CommandBuilder customCommandBuilder) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return RedisFacadeOps.executeToRawStringList(customCommandBuilder.getSr4JCommandTypeExt(),connection, customCommandBuilder.build());
    }

    @Override
    public List<Map<String, String>> queryToListMap(CommandBuilder commandBuilder) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return RedisFacadeOps.queryToListResult(commandBuilder.getSr4JCommandTypeExt(), connection, commandBuilder.build());
    }

    @Override
    public List<Map<String, Object>> queryToListMapType(CommandBuilder commandBuilder, Map<String, SearchListMapTypeOutput.MapType> mapClass) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return RedisFacadeOps.queryToListWithTypeResult(commandBuilder.getSr4JCommandTypeExt(), connection, commandBuilder.build(), mapClass);
    }

    @Override
    public SearchResult<String, String> queryToSearchResult(CommandBuilder commandBuilder) throws RedisCommandExecutionException,ExecutionException, InterruptedException {
        return RedisFacadeOps.queryToSearchResult(commandBuilder.getSr4JCommandTypeExt(), connection, commandBuilder.build());
    }

    @Override
    public List<Map<String, Object>> queryToAggregateResult(CommandBuilder commandBuilder) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return RedisFacadeOps.queryToAggregateResult(commandBuilder.getSr4JCommandTypeExt(), connection, commandBuilder.build());
    }

    @Override
    public String executeForStatus(CommandBuilder commandBuilder) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return RedisFacadeOps.executeForStatus(commandBuilder.getSr4JCommandTypeExt(), connection, commandBuilder.build());
    }

    @Override
    public void close() {
        connection.close();
    }


    @Override
    public boolean setExpire(String key, long expired) throws ExecutionException, InterruptedException {
        return 1L == RedisFacadeOps.executeForInteger(CommandType.EXPIRE, connection, List.of(key, String.valueOf(expired)));
    }
}
