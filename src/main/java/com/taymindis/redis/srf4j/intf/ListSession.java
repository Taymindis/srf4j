package com.taymindis.redis.srf4j.intf;

import com.taymindis.redis.srf4j.RedisOpsMode;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface ListSession<V> extends BaseSession {
    long push(V value ) throws ExecutionException, InterruptedException;

    long push(V value, RedisOpsMode mode) throws ExecutionException, InterruptedException;

    Long push(RedisOpsMode mode, Duration duration, V ...values) throws InterruptedException, ExecutionException, TimeoutException;
    Long push(Duration duration, V ...values) throws InterruptedException, ExecutionException, TimeoutException;
    Long push(V ...values) throws InterruptedException, ExecutionException, TimeoutException;
//    List<Long> push(List<V> values, RedisOpsMode mode, RedisOpsMode pipelineOrTrans, Duration duration) throws InterruptedException, ExecutionException, TimeoutException;

    V pop() throws ExecutionException, InterruptedException;
    List<V> pop(long howMany) throws ExecutionException, InterruptedException;
    V pop(RedisOpsMode mode) throws ExecutionException, InterruptedException;
    List<V> pop(RedisOpsMode mode, long howMany) throws ExecutionException, InterruptedException;


    Long size(String key);

}
