package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.redisearch.CommandBuilder;
import com.taymindis.redis.srf4j.redisearch.CommandTypeExt;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.protocol.CommandType;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Default is All key, values, and result are string based
 * 
 */
abstract class BaseSessionImpl<V>implements AutoCloseable {
    /**
     * Always by default key value/param are UTF 8 String type
     * @return connection;
     */
    private final StatefulConnection<String, V> connection;

    protected BaseSessionImpl(StatefulConnection<String, V> connection) {
        this.connection = connection;
    }

    public StatefulConnection<String, V> getConnection() {
        return this.connection;
    }

    public boolean setExpire(String key, long expired) throws ExecutionException, InterruptedException {
        return 1L == RedisFacadeOps.executeForInteger(CommandType.EXPIRE, connection, List.of(key, String.valueOf(expired)));
    }


    public List<String> executeRawCommand(CommandTypeExt commandType, String query) throws RedisCommandExecutionException,ExecutionException, InterruptedException {
        return RedisFacadeOps.executeToRawStringList(commandType, connection, query);
    }

    public List<String> executeRawCommand(CommandBuilder customCommandBuilder) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return RedisFacadeOps.executeToRawStringList(customCommandBuilder.getSr4JCommandTypeExt(),connection, customCommandBuilder.build());
    }
}
