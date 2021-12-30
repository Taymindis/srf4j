//package com.taymindis.redis.srf4j.intf;
//
//import com.redislabs.lettusearch.Field;
//
//import java.util.Collection;
//import java.util.List;
//
//public interface SearchSession extends AutoCloseable {
//
//    SearchSession createIdx(String indexName, List<Field> fields);
//
////    SearchClient getSearchClientFromIdx(String indName);
//
//    void dropIndex(String indexName);
//
//    SearchSession query(String s, String query);
//
//    SearchSession returnFields(Collection<String> fields);
//
//    SearchSession sortByField(String field, boolean asc);
//
//    SearchSession offset(long num) ;
//
//    SearchSession limit(long num);
//
//    SearchResult execute();
//
//}
