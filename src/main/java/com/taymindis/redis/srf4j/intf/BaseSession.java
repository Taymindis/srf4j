package com.taymindis.redis.srf4j.intf;

import com.taymindis.redis.srf4j.redisearch.CommandBuilder;
import com.taymindis.redis.srf4j.redisearch.CommandTypeExt;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.StatefulConnection;

import java.util.List;
import java.util.concurrent.ExecutionException;

interface BaseSession<V> extends AutoCloseable {

    boolean setExpire(String key, long expired) throws ExecutionException, InterruptedException;

    List<String> executeRawCommand(CommandTypeExt commandType, String query) throws RedisCommandExecutionException, ExecutionException, InterruptedException;

    List<String> executeRawCommand(CommandBuilder customCommandBuilder) throws RedisCommandExecutionException, ExecutionException, InterruptedException;

    StatefulConnection<String, V> getConnection();
}
