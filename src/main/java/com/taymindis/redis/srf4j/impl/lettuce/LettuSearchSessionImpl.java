//package com.taymindis.redis.srf4j.impl.lettuce;
//
//import com.redislabs.lettusearch.*;
//import com.taymindis.redis.srf4j.intf.SearchSession;
//import com.taymindis.redis.srf4j.intf.SearchResult;
//
//import java.util.Collection;
//import java.util.List;
//
//public class LettuSearchSessionImpl implements SearchSession {
//
//    private final StatefulRediSearchConnection<String, Object> connection;
//    private Collection<String> returnFields = null;
//    private String sortField = null, idxName = null;
//    private boolean asc = true;
//    private long limit = -1;
//    private long offset = -1;
//    private String query = null;
//
//    public LettuSearchSessionImpl(StatefulRediSearchConnection<String, Object> connection) {
//        this.connection = connection;
//    }
//
//
//    // @Override
//    // public void close() {
//    //     LOGGER.warn("search Closing pool will be closing all the pools");
//    // }
//
////    public static void shutdownAll() {
////        for(Map.Entry<String, RedisIndexSearch> t: indexedCache.entrySet()) {
////            t.getValue().close();
////        }
////    }
//
//
//    @Override
//    public SearchSession createIdx(String indexName, List<Field> fields) {
//        RediSearchCommands<String, Object> commands = connection.sync();
//        commands.create(indexName, fields.toArray(new Field[0]));
//        return this;
//    }
//
//    @Override
//    public void dropIndex(String indexName) {
//        connection.sync().dropIndex(indexName);
//    }
//
//    @Override
//    public SearchSession query(String idxName, String query) {
//        this.idxName = idxName;
//        this.query = query;
//        return this;
//    }
//
//    public LettuSearchSessionImpl returnFields(Collection<String> fields) {
//        this.returnFields = fields;
//        return this;
//    }
//
//    public LettuSearchSessionImpl sortByField(String field, boolean asc) {
//        this.sortField = field;
//        this.asc = asc;
//        return this;
//    }
//
//    public LettuSearchSessionImpl offset(long num) {
//        this.offset = num;
//        return this;
//    }
//
//    public LettuSearchSessionImpl limit(long num) {
//        this.limit = num;
//        return this;
//    }
//
//    @Override
//    public SearchResult execute() {
//
//        SearchOptions.SearchOptionsBuilder<String> builder = SearchOptions.builder();
//
//        if (idxName == null || query == null)
//            return null;
//
//        if(limit != -1 || offset != -1) {
//            SearchOptions.Limit.LimitBuilder limitBuilder = SearchOptions.Limit.builder();
//            limitBuilder.offset(offset > 0 ? offset : 0);
//            limitBuilder.num(limit > 0 ? limit : 10);
//            builder = builder.limit(limitBuilder.build());
//        }
//
//
//        if (returnFields != null) {
//            builder = builder.returnFields(returnFields);
//        }
//
//        if (sortField != null) {
//            builder = builder.sortBy(SearchOptions.SortBy.<String>builder().field(sortField)
//                    .direction(asc ? SearchOptions.SortBy.Direction.Ascending : SearchOptions.SortBy.Direction.Descending)
//                    .build());
//        }
//
//
//        SearchResult searchResult =
//                new LettuSearchResultImpl(connection.sync().search(idxName, query, builder.build()));
//
//        reset();
//        return searchResult;
//
//    }
//
//    private void reset() {
//        idxName = null;
//        returnFields = null;
//        sortField = null;
//        asc = true;
//        limit = -1;
//        offset = -1;
//        query = null;
//    }
//
//
//    @Override
//    public void close() throws Exception {
//        connection.close();
//    }
//}
