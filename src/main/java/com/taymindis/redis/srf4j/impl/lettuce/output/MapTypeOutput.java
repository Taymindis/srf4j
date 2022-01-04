package com.taymindis.redis.srf4j.impl.lettuce.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapTypeOutput<K> extends CommandOutput<K, Object, Map<K, Object>> {
    private static Logger logger = LoggerFactory.getLogger(MapTypeOutput.class);

    private boolean initialized;
    private K key;
    private Map<K, SearchListMapTypeOutput.MapType> declTypeMapList;

    public MapTypeOutput(RedisCodec<K, Object> codec,
                         Map<K, SearchListMapTypeOutput.MapType> declTypeMapList) {
        super(codec, Collections.emptyMap());
        this.declTypeMapList = declTypeMapList;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (key == null) {
            key = (bytes == null) ? null : codec.decodeKey(bytes);
            return;
        }
        Object value = (bytes == null) ? null : codec.decodeValue(bytes);
        if (declTypeMapList.containsKey(key)) {
            SearchListMapTypeOutput.MapType mapType = declTypeMapList.get(key);
            try {
                switch (mapType) {
                    case INT:
                        value = Integer.parseInt((String) value);
                        break;
                    case LONG:
                        value = Long.parseLong((String) value);
                        break;
                    case DOUBLE:
                        value = Double.parseDouble((String) value);
                        break;
                    case FLOAT:
                        value = Float.parseFloat((String) value);
                        break;
                }
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
        }

        output.put(key, value);
        key = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void set(long integer) {
        if (key == null) {
            key = (K) Long.valueOf(integer);
            return;
        }
        Object value = integer;
        output.put(key, value);
        key = null;
    }

    @Override
    public void multi(int count) {
        if (!initialized) {
            output = new LinkedHashMap<>(count / 2, 1);
            initialized = true;
        }
    }
}
