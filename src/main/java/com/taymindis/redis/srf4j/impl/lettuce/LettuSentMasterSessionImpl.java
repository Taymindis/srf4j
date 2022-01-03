package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.intf.SearchResult;
import com.taymindis.redis.srf4j.intf.Session;
import com.taymindis.redis.srf4j.redisearch.CommandBuilder;
import com.taymindis.redis.srf4j.redisearch.CommandTypeExt;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.internal.Exceptions;
import io.lettuce.core.protocol.CommandType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LettuSentMasterSessionImpl extends BaseSessionImpl<String> implements Session, AutoCloseable {

    private final StatefulRedisConnection<String, String> connection;

    public LettuSentMasterSessionImpl(StatefulRedisConnection<String, String> connection) {
        super(connection);
        this.connection = connection;
    }

    @Override
    public Long insert(String key, Map<String, String> keyVal) {
        RedisCommands<String, String> sync = connection.sync();
        return sync.hset(key, keyVal);
    }

    @Override
    public List<Object> insert(String key, Map<String, String> keyVal, int expirable) throws ExecutionException, InterruptedException {
        List<Object> response = new ArrayList<>();
        RedisReactiveCommands<String, String> reactiveCommands = connection.reactive();

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
        return batchExecTransactionReactive(batchPayload, expirable, timeout);
    }

    private List<Object> batchExecTransactionAsync(List<Map<String, String>> batchPayload, int expirable, Duration timeout) {
        RedisAsyncCommands<String, String> asyncCommands = connection.async();
        asyncCommands.multi();
        for (Map<String, String> rowData : batchPayload) {
            if (rowData.get("__key") != null) {
                String key = rowData.remove("__key");
                asyncCommands.hset(key, rowData);
                if (expirable >= 0) {
                    asyncCommands.expire(key, expirable);
                }
            }
        }
        TransactionResult result;
        try {
            if (timeout == null) {
                result = asyncCommands.exec().get();
            } else {
                result = asyncCommands.exec().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
        } catch (Exception var15) {
            throw Exceptions.fromSynchronization(var15);
        }
        return result.stream().collect(Collectors.toList());
    }

    private List<Object> batchExecTransactionReactive(List<Map<String, String>> batchPayload, int expirable, Duration timeout) {
        RedisReactiveCommands<String, String> reactive = connection.reactive();
        List<Object> res = new ArrayList<>();
        CountDownLatch a = new CountDownLatch(1);
        reactive.multi().subscribe(multiResponse -> {
            for (Map<String, String> rowData : batchPayload) {
                if (rowData.get("__key") != null) {
                    String key = rowData.remove("__key");
                    // reactive using Push methodology
                    reactive.hset(key, rowData).subscribe();
                    if (expirable >= 0) {
                        // reactive using Push methodology
                        reactive.expire(key, expirable).subscribe();
                    }
                }
            }

            reactive.exec().subscribe(result -> {
                try {
                    if (result == null || result.isEmpty()) {
                        return;
                    }
                    res.addAll(result.stream().collect(Collectors.toList()));
                } finally {
                    a.countDown();
                }
            });
        });

        try {
            if(timeout == null) {
                a.await();
            } else {
                a.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return res;
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
        List<Mono<?>> monoList = new LinkedList<>();
        List<Object> resp = new LinkedList<>();
        RedisReactiveCommands<String, String> reactiveCommands = connection.reactive();
        try {
            reactiveCommands.setAutoFlushCommands(false);


            for (Map<String, String> rowData : batchPayload) {
                if (rowData.get("__key") != null) {
                    String key = rowData.remove("__key");
                    monoList.add(reactiveCommands.hset(key, rowData));
                    if (expirable >= 0) {
                        monoList.add(reactiveCommands.expire(key, expirable));
                    }
                }
            }
            CountDownLatch c = new CountDownLatch(1);
            Flux.just(monoList.toArray(new Mono[0]))
                    .flatMap(m -> m)
                    .doFinally(signalType -> {
                        c.countDown();
                    })
                    .subscribe(resp::add);

            reactiveCommands.flushCommands();

            c.await();

            return resp;
        } finally {
            reactiveCommands.setAutoFlushCommands(true);
        }
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
