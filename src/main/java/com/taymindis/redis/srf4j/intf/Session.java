package com.taymindis.redis.srf4j.intf;


import com.taymindis.redis.srf4j.redisearch.CommandTypeExt;
import com.taymindis.redis.srf4j.redisearch.CommandBuilder;
import io.lettuce.core.RedisCommandExecutionException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public interface Session extends BaseSession<String> {

    Long insert(String key, Map<String, String> keyVal) throws ExecutionException, InterruptedException;

    List<Object> insert(String key, Map<String, String> keyVal, int expirable) throws ExecutionException, InterruptedException;

    List<Object> batchExecTransaction(List<Map<String, String>> batchPayload);
    List<Object> batchExecTransaction(List<Map<String, String>> batchPayload, int expirable);
    List<Object> batchExecTransaction(List<Map<String, String>> batchPayload, int expirable, Duration time);
    List<Object> batchExecPipeline(List<Map<String, String>> batchPayload) throws InterruptedException;
    List<Object> batchExecPipeline(List<Map<String, String>> batchPayload, int expirable) throws InterruptedException;
    List<Object> batchExecPipeline(List<Map<String, String>> batchPayload, int expirable, Duration time) throws InterruptedException;


    String getFieldValByKey(String key, String field);

    Map<String, String> getAllByKey(String key);

    boolean sqlLoad(String sqlQuery);

    Long publish(String queueName, String message);

    List<String> executeRawCommand(CommandTypeExt commandType, String query) throws RedisCommandExecutionException,ExecutionException, InterruptedException;

    List<String> executeRawCommand(CommandBuilder customCommandBuilder) throws RedisCommandExecutionException,ExecutionException, InterruptedException;

    SearchResult<String,String> queryToSearchResult(CommandBuilder builder) throws RedisCommandExecutionException,ExecutionException, InterruptedException;
    List<Map<String, Object>> queryToAggregateResult(CommandBuilder builder) throws RedisCommandExecutionException,ExecutionException, InterruptedException;

    String executeForStatus(CommandBuilder customCommandBuilder) throws RedisCommandExecutionException,ExecutionException, InterruptedException;

}
