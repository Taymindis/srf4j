package com.taymindis.redis.srf4j.impl.lettuce.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.*;

public class AggregateToListMapOutput<K, V> extends CommandOutput<K, V, List<Map<K, Object>>> implements StreamingOutput<Map<K, V>> {
    protected int sz = 0;
    private Subscriber<Map<K, V>> subscriber;
    private K currentKey;
    private Map<K, Object> child = null;
    private List<V> groupData = null;
    private int currChildCountReq, currGroupDataCountReq;

    public AggregateToListMapOutput(RedisCodec<K, V> codec) {
        super(codec, null);
        this.setSubscriber(ListSubscriber.instance());
    }

    public void set(ByteBuffer bytes) {
        if (this.currentKey == null) {
            this.currentKey = bytes == null ? null : this.codec.decodeKey(bytes);
        } else {
            V value = bytes == null ? null : this.codec.decodeValue(bytes);
            if(this.groupData == null) {
                this.child.put(this.currentKey, value);
                this.currentKey = null;

                if(currChildCountReq == this.child.size()) {
                    doneRow();
                }
            } else {
                this.groupData.add(value);
                if(this.groupData.size() == currGroupDataCountReq) {
                    doneReduce();
                }

            }
        }
    }

    private void doneReduce() {
        this.child.put(this.currentKey, groupData);
        if(currChildCountReq == this.child.size()) {
            doneRow();
        }
        this.groupData = null;
        this.currentKey = null;
        this.currGroupDataCountReq = 0;
    }

    private void doneRow() {
        this.output.add(child);
        this.child = null;
        this.currentKey = null;
        this.currChildCountReq = 0;
    }


//    public void complete(int depth) {
//        if (!this.counts.isEmpty()) {
//            int expectedSize = (Integer)this.counts.get(0);
//            if (((Map)this.nested.get()).size() == expectedSize) {
//                this.counts.remove(0);
//                ((AggregateResults)this.output).add(new LinkedHashMap((Map)this.nested.get()));
//                ((Map)this.nested.get()).clear();
//            }
//        }
//
//    }

    public void set(long sz) {
        if (this.output == null) {
            this.output = new ArrayList<Map<K, Object>>((int) sz);
        }
    }

    public void multi(int count) {
        if (this.output == null) {
            return;
        }

        if(this.currentKey == null ) {
            this.currChildCountReq = count / 2;
            this.child = new HashMap<>(this.currChildCountReq);
        } else {
            this.groupData = new ArrayList<>(count);
            this.currGroupDataCountReq = count;
        }
    }

    public void setSubscriber(Subscriber<Map<K, V>> subscriber) {
        LettuceAssert.notNull(subscriber, "Subscriber must not be null");
        this.subscriber = subscriber;
    }

    public Subscriber<Map<K, V>> getSubscriber() {
        return this.subscriber;
    }
}