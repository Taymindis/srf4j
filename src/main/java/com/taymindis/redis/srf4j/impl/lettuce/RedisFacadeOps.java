package com.taymindis.redis.srf4j.impl.lettuce;

import com.redislabs.lettusearch.SearchResults;
import com.redislabs.lettusearch.output.SearchNoContentOutput;
import com.redislabs.lettusearch.output.SearchOutput;
import com.taymindis.redis.srf4j.impl.lettuce.output.AggregateToListMapOutput;
import com.taymindis.redis.srf4j.impl.lettuce.output.GenericRawStringListOutput;
import com.taymindis.redis.srf4j.impl.lettuce.output.SearchListMapOutput;
import com.taymindis.redis.srf4j.intf.SearchResult;

import com.taymindis.redis.srf4j.redisearch.CommandTypeExt;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RedisFacadeOps {

    static <V> List<String> executeToRawStringList(ProtocolKeyword commandType, StatefulConnection<String, V> connection,
                                               String query) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return executeToRawStringList(commandType, connection, Arrays.asList(query.split("\\s")));
    }

    static <V> List<String> executeToRawStringList(ProtocolKeyword commandType, StatefulConnection<String, V> connection,
                                               List<CharSequence> values) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        RedisCodec<String, String> codec = StringCodec.UTF8;
        try (Stream<CharSequence> v = values.stream()) {
            RedisCommand<String, V, List<String>> idxInfoCmd =
                    new Command(commandType,
                            // Cannot make it...
                            new GenericRawStringListOutput<>(codec),
                            new CommandArgs<>(codec)
                                    .addValues(v.map(CharSequence::toString).collect(Collectors.toList())));

            AsyncCommand<String, V, List<String>> async = new AsyncCommand<>(idxInfoCmd);

            connection.dispatch(async);

            async.await(-1, TimeUnit.SECONDS);


            return async.get();
        }
    }


    static String executeForStatus(ProtocolKeyword commandType, StatefulConnection<String, String> connection,
                                   String query) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return executeForStatus(commandType, connection, Arrays.asList(query.split("\\s")));
    }

    static String executeForStatus(ProtocolKeyword commandType, StatefulConnection<String, String> connection,
                                   List<CharSequence> values) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        RedisCodec<String, String> codec = StringCodec.UTF8;
        try (Stream<CharSequence> v = values.stream()) {
            RedisCommand<String, String, String> idxInfoCmd =
                    new Command<>(commandType,
                            // Cannot make it...
                            new StatusOutput<>(codec),
                            new CommandArgs<>(codec)
                                    .addValues(v.map(CharSequence::toString).collect(Collectors.toList())));

            AsyncCommand<String, String, String> async = new AsyncCommand<>(idxInfoCmd);

            connection.dispatch(async);

            async.await(-1, TimeUnit.SECONDS);

            return async.get();
        }
    }

    static Long executeForInteger(ProtocolKeyword commandType, StatefulConnection connection,
                                  List<CharSequence> values) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        RedisCodec<String, String> codec = StringCodec.UTF8;
        try (Stream<CharSequence> v = values.stream()) {
            RedisCommand<String, String, Long> idxInfoCmd =
                    new Command<>(commandType,
                            // Cannot make it...
                            new IntegerOutput<>(codec),
                            new CommandArgs<>(codec)
                                    .addValues(v.map(CharSequence::toString).collect(Collectors.toList())));

            AsyncCommand<String, String, Long> async = new AsyncCommand<>(idxInfoCmd);

            connection.dispatch(async);

            async.await(-1, TimeUnit.SECONDS);

            return async.get();
        }
    }

    static SearchResult<String, String> queryToSearchResult(ProtocolKeyword commandType, StatefulConnection<String, String> connection,
                                                            String query) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return queryToSearchResult(commandType, connection, Arrays.asList(query.split("\\s")));
    }


    static SearchResult<String, String> queryToSearchResult(ProtocolKeyword commandType, StatefulConnection<String, String> connection,
                                                            List<CharSequence> values) throws RedisCommandExecutionException, ExecutionException, InterruptedException {

        String valString = String.join(" ", values).toUpperCase();

        return queryToSearchResult(commandType, connection,
                valString.contains(" NOCONTENT"),
                valString.contains(" WITHSCORES"),
                valString.contains(" WITHSORTKEYS"),
                valString.contains(" WITHPAYLOADS"),
                values
        );
    }

    private static <K, V> SearchResult<K, V> queryToSearchResult(
            ProtocolKeyword commandType, StatefulConnection<String, String> connection,
            boolean noContent, boolean withScores, boolean withSortKeys, boolean withPayloads,
            List<CharSequence> values)
            throws RedisCommandExecutionException, ExecutionException, InterruptedException {

        RedisCodec<String, String> codec = StringCodec.UTF8;
        try (Stream<CharSequence> v = values.stream()) {
            // Redis Cluster command ...
            RedisCommand<String, String, SearchResults<K, V>> idxInfoCmd =
                    new Command(commandType,
                            // Cannot make it...
                            noContent
                                    ? new SearchNoContentOutput<>(codec, withScores)
                                    : new SearchOutput<>(codec, withScores, withSortKeys, withPayloads),
                            new CommandArgs<>(codec)
                                    .addValues(v.map(CharSequence::toString).collect(Collectors.toList())));

            AsyncCommand<String, String, SearchResults<K, V>> async = new AsyncCommand<>(idxInfoCmd);

            connection.dispatch(async);

            async.await(-1, TimeUnit.SECONDS);

            SearchResults<K, V> searchResults = async.get();

            return new LettuSearchResultImpl<>(searchResults);
        }
    }

    static List<Map<String, String>> queryToListResult(ProtocolKeyword commandType, StatefulConnection<String, String> connection,
                                                            String query) throws RedisCommandExecutionException, ExecutionException, InterruptedException {
        return queryToListResult(commandType, connection, Arrays.asList(query.split("\\s")));
    }


    static List<Map<String, String>> queryToListResult(ProtocolKeyword commandType, StatefulConnection<String, String> connection,
                                                            List<CharSequence> values) throws RedisCommandExecutionException, ExecutionException, InterruptedException {

        String valString = String.join(" ", values).toUpperCase();

        return queryToListResult(commandType, connection,
                valString.contains(" NOCONTENT"),
                valString.contains(" WITHSCORES"),
                valString.contains(" WITHSORTKEYS"),
                valString.contains(" WITHPAYLOADS"),
                values
        );
    }

    private static <K, V> List<Map<K,V>> queryToListResult(
            ProtocolKeyword commandType, StatefulConnection<String, String> connection,
            boolean noContent, boolean withScores, boolean withSortKeys, boolean withPayloads,
            List<CharSequence> values)
            throws RedisCommandExecutionException, ExecutionException, InterruptedException {

        RedisCodec<String, String> codec = StringCodec.UTF8;
        try (Stream<CharSequence> v = values.stream()) {
            // Redis Cluster command ...
            RedisCommand<String, String, List<Map<K, V>>> idxInfoCmd =
                    new Command(commandType,
                            // Cannot make it...
                            noContent
                                    ? new SearchNoContentOutput<>(codec, withScores)
                                    : new SearchListMapOutput(codec, withScores, withSortKeys, withPayloads),
                            new CommandArgs<>(codec)
                                    .addValues(v.map(CharSequence::toString).collect(Collectors.toList())));

            AsyncCommand<String, String, List<Map<K, V>>> async = new AsyncCommand<>(idxInfoCmd);

            connection.dispatch(async);

            async.await(-1, TimeUnit.SECONDS);

            List<Map<K, V>> searchResults = async.get();

            return searchResults;
        }
    }

//    static <V> AggregateResults<V> queryToAggregateResult(
//            SR4JProtocolKeyword commandType, StatefulConnection<String, String> connection,
//            List<CharSequence> values)
//            throws RedisCommandExecutionException, ExecutionException, InterruptedException {
//
//        if (commandType != SR4JProtocolKeyword.FT_AGGREGATE) {
//            return null;
//        }
//
//        RedisCodec<String, String> codec = StringCodec.UTF8;
//        try (Stream<CharSequence> v = values.stream()) {
//            RedisCommand<String, String, AggregateResults<V>> idxInfoCmd =
//                    new Command(commandType,
//                             new AggregateOutput<>(codec, new AggregateResults<>()),
//                              new CommandArgs<>(codec)
//                                    .addValues(v.map(CharSequence::toString)
//                                            .collect(Collectors.toList())));
//
//            AsyncCommand<String, String, AggregateResults<V>> async = new AsyncCommand<>(idxInfoCmd);
//
//            RedisCommand<String, String, AggregateResults<V>> dispatched = connection.dispatch(async);
//
//            async = (dispatched instanceof AsyncCommand ? (AsyncCommand<String, String, AggregateResults<V>>)dispatched : async);
//
//            async.await(-1, TimeUnit.SECONDS);
//
//            return async.get();
//        }
//    }

    static List<Map<String, Object>> queryToAggregateResult(
            ProtocolKeyword commandType, StatefulConnection<String, String> connection,
            List<CharSequence> values)
            throws RedisCommandExecutionException, ExecutionException, InterruptedException {

        if (commandType != CommandTypeExt.FT_AGGREGATE) {
            return null;
        }

        RedisCodec<String, String> codec = StringCodec.UTF8;
        try (Stream<CharSequence> v = values.stream()) {
            RedisCommand<String, String, List<Map<String, Object>>> idxInfoCmd =
                    new Command<>(commandType,
                            new AggregateToListMapOutput<>(codec),
                            new CommandArgs<>(codec)
                                    .addValues(v.map(CharSequence::toString)
                                            .collect(Collectors.toList())));

            AsyncCommand<String, String, List<Map<String, Object>>> async = new AsyncCommand<>(idxInfoCmd);

            RedisCommand<String, String, List<Map<String, Object>>> dispatched = connection.dispatch(async);

            async = (dispatched instanceof AsyncCommand ? (AsyncCommand<String, String, List<Map<String, Object>>>)dispatched : async);

            async.await(-1, TimeUnit.SECONDS);

            return async.get();
        }
    }


}
