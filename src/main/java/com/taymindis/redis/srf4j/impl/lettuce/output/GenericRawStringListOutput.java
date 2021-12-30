package com.taymindis.redis.srf4j.impl.lettuce.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.*;

public class GenericRawStringListOutput<K, V> extends CommandOutput<K, V, List<String>> implements StreamingOutput<String> {
    private boolean initialized;
    private Subscriber<String> subscriber;

    public GenericRawStringListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<>());
        this.setSubscriber(ListSubscriber.instance());
    }

    public void set(ByteBuffer bytes) {
        this.subscriber.onNext(this.output, bytes == null ? null :  this.decodeAscii(bytes));
    }

    public void setBigNumber(ByteBuffer bytes) {
        this.set(bytes);
    }

    public void set(long integer) {
        String value = String.valueOf(integer);
        this.output.add(value);
    }

    public void set(double number) {
        String value =String.valueOf(number);
        this.output.add(value);
    }

    public void multi(int count) {
        if (!this.initialized) {
//            this.output = new ArrayList<>();
            this.initialized = true;
        }
    }

    public void setSubscriber(Subscriber<String> subscriber) {
        LettuceAssert.notNull(subscriber, "Subscriber must not be null");
        this.subscriber = subscriber;
    }

    public Subscriber<String> getSubscriber() {
        return this.subscriber;
    }
}
