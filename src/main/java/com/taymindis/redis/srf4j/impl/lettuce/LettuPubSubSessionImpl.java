package com.taymindis.redis.srf4j.impl.lettuce;

import com.taymindis.redis.srf4j.intf.PubSubEvent;
import com.taymindis.redis.srf4j.intf.PubSubSession;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.pubsub.api.async.RedisClusterPubSubAsyncCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;

import java.time.Duration;
import java.util.*;


public class LettuPubSubSessionImpl<K, V> implements PubSubSession<K, V> {

    private final StatefulRedisPubSubConnection<K, V> connection;
    private Set<K> channels;
    private Set<K> pchannels;
    private Set<PubSubEvent<K, V>> pubSubEvents;

    public LettuPubSubSessionImpl(
            StatefulRedisPubSubConnection<K, V> connection,
            PubSubEvent<K, V>... pubSubEventArgs) {
        this.connection = connection;
        this.channels = new HashSet<>();
        this.pchannels = new HashSet<>();
        this.pubSubEvents = new HashSet<>();
        for (PubSubEvent<K, V> pse :
                pubSubEventArgs) {
            connection.addListener(pse);
        }
        Collections.addAll(this.pubSubEvents, pubSubEventArgs);
    }

    /**
     * No Remove listener, do not make it too complicated
     **/
    public void removeListener() {
//        connection.removeListener();
    }

    @Override
    public void close() throws Exception {
        RedisPubSubAsyncCommands<K, V> async
                = connection.async();

        List<RedisFuture<Void>> sets = new LinkedList<>();
        for (K c :
                this.channels) {
            sets.add(async.unsubscribe(c));
        }

        for (K c :
                this.pchannels) {
            sets.add(async.punsubscribe(c));
        }

        LettuceFutures.awaitAll(Duration.ofSeconds(0), sets.toArray(new RedisFuture[0]));

        for (PubSubEvent<K, V> pse :
                this.pubSubEvents) {
            connection.removeListener(pse);
        }
        connection.close();
    }


    @Override
    public void subscribe(K... channel) {
        if (channel != null && channel.length > 0) {
            RedisPubSubAsyncCommands<K, V> async
                    = connection.async();
            async.subscribe(channel);
            Collections.addAll(channels, channel);
        }
    }

    @Override
    public void unsubscribe(K... channel) {
        RedisPubSubAsyncCommands<K, V> async
                = connection.async();
        if (channel != null && channel.length > 0) {
            async.unsubscribe(channel);
            channels.removeAll(Arrays.asList(channel));
        }
    }

    /**
     It is pattern subscription
     * @param pchannel example
     *  h?llo subscribes to hello, hallo and hxllo
     *  h*llo subscribes to hllo and heeeello
     *  h[ae]llo subscribes to hello and hallo, but not hillo
     */
    @Override
    public void psubscribe(K... pchannel) {
        if (pchannel != null && pchannel.length > 0) {
            RedisPubSubAsyncCommands<K, V> async
                    = connection.async();
            async.psubscribe(pchannel);
            Collections.addAll(pchannels, pchannel);
        }
    }

    @Override
    public void punsubscribe(K... pchannel) {
        RedisPubSubAsyncCommands<K, V> async
                = connection.async();
        if (pchannel != null && pchannel.length > 0) {
            async.punsubscribe(pchannel);
            pchannels.removeAll(Arrays.asList(pchannel));
        }
    }

    @Override
    public void publish(K channel, V value) {
        RedisPubSubAsyncCommands<K, V> async
                = connection.async();
        async.publish(channel, value);
    }
}
