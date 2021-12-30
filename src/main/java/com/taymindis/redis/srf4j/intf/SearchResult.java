package com.taymindis.redis.srf4j.intf;

public interface SearchResult<K, V> {
 int size();
 V get(int index, K key);
 Object get(int index);
 Object getResult();
 boolean isEmpty();
}
