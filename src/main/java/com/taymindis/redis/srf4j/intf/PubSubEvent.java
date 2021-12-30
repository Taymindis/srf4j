package com.taymindis.redis.srf4j.intf;

import io.lettuce.core.pubsub.RedisPubSubListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class PubSubEvent<K,V> implements RedisPubSubListener<K,V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubEvent.class.getName());

    public abstract void message(K channel, V value);

    public void message(K pchannel, K channel, V value) {
        this.message(channel, value);
    }

    public void subscribed(K channel, long value) {
        LOGGER.debug("Event has subscribed {}", channel);
    }

    public void psubscribed(K channel, long value){
        LOGGER.debug("Event has psubscribed {}", channel);
    }

    public void unsubscribed(K channel, long value){
        LOGGER.debug("Event has unsubscribed {}", channel);
    }

    public void punsubscribed(K channel, long value){
        LOGGER.debug("Event has punsubscribed {}", channel);
    }

}
