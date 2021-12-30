package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.RedisOpsMode;
import com.taymindis.redis.srf4j.intf.*;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.CommandType;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LettuListSessionImpl<V> extends BaseSessionImpl<V> implements ListSession<V> {

    private final StatefulRedisConnection<String, V> connection;
    private final String key;

    public LettuListSessionImpl(String key, StatefulRedisConnection<String, V> connection) {
        super(connection);
        this.connection = connection;
        this.key = key;
    }

    @Override
    public long push(V value) throws ExecutionException, InterruptedException {
        return push(value, RedisOpsMode.LEFT_IN);
    }

    @Override
    public Long push(V... values) throws InterruptedException, ExecutionException, TimeoutException {
        return this.push(RedisOpsMode.LEFT_IN, Duration.ofSeconds(1800), values);
    }

    @Override
    public Long push(Duration duration, V... values) throws InterruptedException, ExecutionException, TimeoutException {
        return this.push(RedisOpsMode.LEFT_IN, duration, values);
    }

    @Override
    public V pop() throws ExecutionException, InterruptedException {
        return pop(RedisOpsMode.RIGHT_OUT);
    }

    @Override
    public List<V> pop(long howMany) throws ExecutionException, InterruptedException {
        return pop(RedisOpsMode.RIGHT_OUT, howMany);
    }

    @Override
    public long push(V value, RedisOpsMode mode) throws ExecutionException, InterruptedException {
        RedisAsyncCommands<String, V> commands = connection.async();
        final RedisFuture<Long> future;

        switch (mode) {
            case RIGHT_IN:
                future = commands.rpush(key, value);
                break;
            case LEFT_IN:
            default:
                future = commands.lpush(key, value);
        }
        return future.get();
    }

//    @Override
//    public List<Long> push(List<V> values, RedisOpsMode mode, RedisOpsMode pipelineOrTrans, Duration duration) throws InterruptedException, ExecutionException, TimeoutException {
//        RedisAsyncCommands<String, V> commands = connection.async();
//         List<RedisFuture<Long>> futures = null;
//
//        switch (pipelineOrTrans){
//            case PIPELINE:
//                futures = new ArrayList<>();
//                commands.setAutoFlushCommands(false);
//                switch (mode){
//                    case RIGHT_IN:
//                        for (V value :
//                                values) {
//                            futures.add(commands.rpush(key, value));
//                        }
//                        break;
//                    case LEFT_IN:
//                    default:
//                        for (V value :
//                                values) {
//                            futures.add(commands.lpush(key, value));
//                        }
//                }
//                break;
//            case TRANSACTION:
//                commands.multi();
//                switch (mode){
//                    case RIGHT_IN:
//                        for (V value :
//                                values) {
//                            commands.rpush(key, value);
//                        }
//                        break;
//                    case LEFT_IN:
//                    default:
//                        for (V value :
//                                values) {
//                            commands.lpush(key, value);
//                        }
//                }
//            default:
//        }
//
//        switch (pipelineOrTrans){
//            case PIPELINE:
//                commands.flushCommands();
//                LettuceFutures.awaitAll(duration, futures.toArray(new RedisFuture[0]));
//
//                return futures.stream().map(f -> {
//                    try {
//                        return f.get();
//                    } catch (InterruptedException | ExecutionException e) {
//                        e.printStackTrace();
//                    }
//                    return null;
//                }).collect(Collectors.toList());
//
//            case TRANSACTION:
//               TransactionResult result = commands.exec().get(duration.toNanos(), TimeUnit.NANOSECONDS);
//               return result.stream().map(o -> Long.parseLong(String.valueOf(o))).collect(Collectors.toList());
//            default:
//        }
//
//        return null;
//    }

    @Override
    public Long push(RedisOpsMode mode, Duration duration, V... values) throws InterruptedException, ExecutionException, TimeoutException {
        RedisAsyncCommands<String, V> commands = connection.async();
        final RedisFuture<Long> future;

        switch (mode) {
            case RIGHT_IN:
                future = commands.rpush(key, values);
                break;
            case LEFT_IN:
            default:
                future = commands.lpush(key, values);
        }

        return future.get(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public V pop(RedisOpsMode mode) throws ExecutionException, InterruptedException {
        RedisCommands<String, V> commands = connection.sync();
        final V value;

        switch (mode) {
            case LEFT_OUT:
                value = commands.lpop(key);
                break;
            case RIGHT_OUT:
            default:
                value = commands.rpop(key);
        }
        return value;
    }

    @Override
    public List<V> pop(RedisOpsMode mode, long howMany) throws ExecutionException, InterruptedException {
        RedisCommands<String, V> commands = connection.sync();
        final List<V> value;
        switch (mode) {
            case LEFT_OUT:
                value = commands.lpop(key, howMany);
                break;
            case RIGHT_OUT:
            default:
                value = commands.rpop(key, howMany);
        }
        return value;
    }

    @Override
    public Long size(String key) {
        RedisCommands<String, V> commands = connection.sync();
        return commands.llen(key);
    }

    @Override
    public boolean setExpire(String key, long expired) throws ExecutionException, InterruptedException {
        return 1L == RedisFacadeOps.executeForInteger(CommandType.EXPIRE, connection, List.of(key, String.valueOf(expired)));
    }

    @Override
    public void close() {
        connection.close();
    }
}
