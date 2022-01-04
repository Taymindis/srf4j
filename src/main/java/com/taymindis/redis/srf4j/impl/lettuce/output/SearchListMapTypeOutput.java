package com.taymindis.redis.srf4j.impl.lettuce.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.MapOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchListMapTypeOutput<K, V> extends CommandOutput<K, V, List<Map<K, Object>>> {
    private final List<Integer> counts;
    private final boolean withScores;
    private final boolean withSortKeys;
    private final boolean withPayloads;
    private MapTypeOutput<K> nested;
    private Map<String, Object> current;
    private Map<String, SearchListMapTypeOutput.MapType> declTypeMapList;
    private int mapCount;
    private boolean payloadSet;
    private boolean scoreSet;


    public static enum MapType {
        INT,LONG,DOUBLE,FLOAT
    }

    public SearchListMapTypeOutput(RedisCodec<K, V> codec,Map<String, SearchListMapTypeOutput.MapType> declTypeMapList) {
        this(codec, declTypeMapList, false, false, false);
    }

    public SearchListMapTypeOutput(RedisCodec<K, V> codec, Map<String, SearchListMapTypeOutput.MapType> declTypeMapList, boolean withScores, boolean withSortKeys, boolean withPayloads) {
        super(codec, null);
        this.declTypeMapList = declTypeMapList;
        this.counts = new ArrayList();
        this.mapCount = -1;
        this.payloadSet = false;
        this.scoreSet = false;
        this.nested = new MapTypeOutput(codec, declTypeMapList);
        this.withScores = withScores;
        this.withSortKeys = withSortKeys;
        this.withPayloads = withPayloads;
    }

    public void set(ByteBuffer bytes) {
        if (this.current == null) {
            this.current = new HashMap<>();
            this.payloadSet = false;
            this.scoreSet = false;
            if (bytes != null) {
                this.current.put("_id", this.codec.decodeKey(bytes));
            }

            ((List)this.output).add(this.current);
        } else if (this.withSortKeys && this.current.get("sortKey") == null) {
            if (bytes != null) {
                this.current.put("sortKey", this.codec.decodeValue(bytes));
            }
        } else if (this.withPayloads && !this.payloadSet) {
            if (bytes != null) {
                this.current.put("payload",this.codec.decodeValue(bytes));
            }

            this.payloadSet = true;
        } else {
            this.nested.set(bytes);
        }

    }

    public void set(long sz) {
        if (this.output == null) {
            this.output = new ArrayList<Map<K, Object>>((int) sz);
        }
    }

    public void set(double number) {
        if (this.withScores && !this.scoreSet) {
            this.current.put ("score", number);
            this.scoreSet = true;
        }

    }

    public void complete(int depth) {
        if (!this.counts.isEmpty() && ((Map)this.nested.get()).size() == (Integer)this.counts.get(0)) {
            this.counts.remove(0);
            this.current.putAll((Map)this.nested.get());
            this.nested = new MapTypeOutput(this.codec, this.declTypeMapList);
            this.current = null;
            this.payloadSet = false;
            this.scoreSet = false;
        }

    }

    public void multi(int count) {
        this.nested.multi(count);
        if (this.mapCount == -1) {
            this.mapCount = count;
        } else {
            this.counts.add(count / 2);
        }

    }
}