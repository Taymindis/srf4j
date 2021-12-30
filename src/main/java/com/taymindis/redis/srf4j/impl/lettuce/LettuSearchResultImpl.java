package com.taymindis.redis.srf4j.impl.lettuce;

import com.redislabs.lettusearch.SearchResults;

public class LettuSearchResultImpl<K,V> implements com.taymindis.redis.srf4j.intf.SearchResult<K,V> {
    private final SearchResults<K,V> searchResult;

    public LettuSearchResultImpl(SearchResults<K,V> searchResult) {
        this.searchResult = searchResult;
    }

    @Override
    public int size() {
        return searchResult.size();
    }

    @Override
    public V get(int index, K key) {
        return searchResult.get(index).get(key);
    }

    @Override
    public Object get(int index) {
        return searchResult.get(index);
    }

    @Override
    public SearchResults<K, V> getResult() {
        return searchResult;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }
}
