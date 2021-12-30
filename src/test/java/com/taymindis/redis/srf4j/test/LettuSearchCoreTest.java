package com.taymindis.redis.srf4j.test;

import com.redislabs.lettusearch.*;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LettuSearchCoreTest {

    @Test
    public void testSimpleSearch() {


        // Example 1. Redis Sentinel node connection
//        RedisURI redisUri = RedisURI.create("redis://sentinelhost1:26379");
//        RedisClient client = new RedisClient(redisUri);
//        RedisSentinelAsyncConnection<String, String>  connection = client.connectSentinelAsync();
//        Map<String, String> map = connection.master("mymaster").get();


//        RedisURI redisUri = RedisURI.Builder.sentinel("localhost", 6379, "mymaster").withSentinel("localhost", 7379).build();
        RedisURI redisUri = RedisURI.Builder.redis("localhost", 6379).build();
        RediSearchClient client = RediSearchClient.create(redisUri); // (1)
        StatefulRediSearchConnection<String, String> connection = client.connect(); // (2)
        RediSearchCommands<String, String> commands = connection.sync(); // (3)

//        commands.create("cust", Field.text("firstname").build()); // (4)
//        commands.hmset("beer:1", Map.of("name", "Chouffe")); // (5)

//        ProtocolKeyword e = new ProtocolKeyword() {
//            @Override
//            public byte[] getBytes() {
//                return "idx/retailer/cust".getBytes();
//            }
//
//            @Override
//            public String name() {
//                return "idx/retailer/cust";
//            }
//        };

        SearchOptions.SearchOptionsBuilder<String> builder = SearchOptions.builder();

        SearchOptions<String> a = builder.limit(SearchOptions.Limit.builder()
                .num(1).offset(1)
                .build())
                .returnFields(List.of("phone", "custId", "firstName"))
                .build();

//        SearchResults<String, String> results =
//                commands.search("idx/retailer/cust", "@firstName:Dale",
//                        a); // (6)
        SearchResults<String, String> results =
                commands.search("database_idx", "Red*"//,a
                ); // (6)
        System.out.println("Found " + results.getCount() + " documents matching query");
        for (Document<String, String> doc : results) {
            System.out.println(doc);
        }
    }

    @Test
    public void testSentinelSearch() throws ExecutionException, InterruptedException {
        // Example 1. Redis Sentinel node connection
        Boolean useSentinel = false;

        RedisURI redisUri;
        if(useSentinel) {
             redisUri = RedisURI.Builder.sentinel("localhost", 26379, "mymaster")
                    .withSentinel("localhost", 26479)
                    .withSentinel("localhost", 26579).build();
        } else {
            redisUri = RedisURI.Builder.redis("localhost", 6379).build();
        }


        RediSearchClient client = RediSearchClient.create(redisUri); // (1)
        StatefulRediSearchConnection<String, String> connection = client.connect(); // (2)
        RediSearchAsyncCommands<String, String> commands = connection.async(); // (3)

        RedisFuture<AggregateResults<String>> future = commands.aggregate("idx/retailer/custOrderss", "@custId:(1|2|3|4)",
                AggregateOptions.builder()
                        .groupBy(List.of("custId"),
                                AggregateOptions.Operation.GroupBy.Reducer
                                        .FirstValue.builder().property("firstName").as("name").build(),
                                AggregateOptions.Operation.GroupBy.Reducer
                                        .ToList.builder().property("orderId").as("orderIds").build(),
                                AggregateOptions.Operation.GroupBy.Reducer
                                        .ToList.builder().property("comments").as("comments").build()
                        ).build()
        );

        LettuceFutures.awaitAll(Duration.ofSeconds(100), future);


        AggregateResults<String> res = future.get();


        if (!res.isEmpty()) {
            boolean gotCommentGood = false;
            for (Map<String, Object> doc : res) {
                Object comments = doc.get("comments");
                if (comments instanceof List) {
                    gotCommentGood = ((ArrayList<String>) comments).get(0).equals("good");
                }
                System.out.println(doc);
            }
            Assertions.assertTrue(gotCommentGood, "No Comment Good found");
        }
    }
}
