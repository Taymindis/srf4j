package com.taymindis.redis.srf4j.intf;

public interface PubSubSession<K, V> extends AutoCloseable {
    void subscribe(K... channel);

    void unsubscribe(K... channel);

    void psubscribe(K... pchannel);

    void punsubscribe(K... pchannel);

    void publish(K channel, V value);
}
