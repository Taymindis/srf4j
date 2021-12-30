package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.RedisOpsMode;
import com.taymindis.redis.srf4j.intf.ListSession;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.protocol.CommandType;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LettuClusterListSessionImpl<V> extends BaseSessionImpl<V> implements ListSession<V> {

    private final StatefulRedisClusterConnection<String, V> connection;
    private final String key;

    public LettuClusterListSessionImpl(String key, StatefulRedisClusterConnection<String, V> connection) {
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
        RedisClusterAsyncCommands<String, V> commands = connection.async();
        final RedisFuture<Long> future;

        switch (mode){
            case RIGHT_IN:
                future =  commands.rpush(key, value);
                break;
            case LEFT_IN:
            default:
                future =  commands.lpush(key, value);
        }
        return future.get();
    }

//    @Override
//    public List<Long> push(List<V> values, RedisOpsMode mode, RedisOpsMode pipelineOrTrans, Duration duration) throws InterruptedException, ExecutionException, TimeoutException {
//        RedisClusterAsyncCommands<String, V> commands = connection.async();
//         List<RedisFuture<Long>> futures = null;
//
//        switch (pipelineOrTrans){
//            case PIPELINE:
//                futures = new ArrayList<>();
//                commands.setAutoFlushCommands(false);
//                break;
//            case TRANSACTION:
//                throw new UnsupportedOperationException("Transaction is not support by Cluster Session");
//            default:
//        }
//
//
//
//
//        switch (mode){
//            case RIGHT_IN:
//                for (V value :
//                        values) {
//                    futures.add(commands.rpush(key, value));
//                }
//                break;
//            case LEFT_IN:
//            default:
//                for (V value :
//                        values) {
//                    futures.add(commands.lpush(key, value));
//                }
//        }
//
//
//
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
//            default:
//        }
//
//        return null;
//    }

    @Override
    public Long push(RedisOpsMode mode, Duration duration, V... values) throws InterruptedException, ExecutionException, TimeoutException {
        RedisClusterAsyncCommands<String, V> commands = connection.async();
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
        RedisClusterCommands<String, V> commands = connection.sync();
        final V value;

        switch (mode){
            case LEFT_OUT:
                value =  commands.lpop(key);
                break;
            case RIGHT_OUT:
            default:
                value =  commands.rpop(key);
        }
        return value;
    }

    @Override
    public List<V> pop(RedisOpsMode mode, long howMany) throws ExecutionException, InterruptedException {
        RedisClusterCommands<String, V> commands = connection.sync();
        final List<V> value;
        switch (mode){
            case LEFT_OUT:
                value = commands.lpop(key, howMany);
                break;
            case RIGHT_OUT:
            default:
                value =  commands.rpop(key, howMany);
        }
        return value;
    }

    @Override
    public Long size(String key) {
        RedisClusterCommands<String, V> commands = connection.sync();
        return commands.llen(key);
    }

    @Override
    public boolean setExpire(String key, long expired) throws ExecutionException, InterruptedException {
//        CustomCommandBuilder customCommandBuilder = new CustomCommandBuilder(SRF4JCommandTypeExt.EXPIRE)
//                .addIdxOrQuery(key, String.valueOf(expired));
        return 1L == RedisFacadeOps.executeForInteger(CommandType.EXPIRE, connection, List.of(key, String.valueOf(expired)));
    }

    @Override
    public void close() {
        connection.close();
    }

}
