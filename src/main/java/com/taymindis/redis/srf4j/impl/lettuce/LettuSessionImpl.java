package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.intf.SearchResult;
import com.taymindis.redis.srf4j.intf.Session;
import com.taymindis.redis.srf4j.redisearch.CommandBuilder;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.internal.Exceptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LettuSessionImpl extends BaseSessionImpl<String> implements Session, AutoCloseable {

    private final StatefulRedisConnection<String, String> connection;

    public LettuSessionImpl(StatefulRedisConnection<String, String> connection) {
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
        RedisReactiveCommands<String, String> async = connection.reactive();
        async.multi();
        response.add(async.hset(key, keyVal));
        if (expirable >= 0) {
            response.add(async.expire(key, expirable));
        }
        async.exec().block();
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

    @Override
    public List<Object> batchExecPipeline(List<Map<String, String>> batchPayload) {
        return batchExecPipeline(batchPayload, -1);
    }

    @Override
    public List<Object> batchExecPipeline(List<Map<String, String>> batchPayload, int expirable) {
        return batchExecPipeline(batchPayload, expirable, null);
    }

    @Override
    public List<Object> batchExecPipeline(List<Map<String, String>> batchPayload, int expirable, Duration timeout) {
        List<RedisFuture<?>> response = new ArrayList<>();
        RedisAsyncCommands<String, String> async = connection.async();
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
            e.printStackTrace();
        } finally {
            async.setAutoFlushCommands(true);
        }
        return null;
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
}
