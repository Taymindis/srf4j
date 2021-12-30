package com.taymindis.redis.srf4j.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.*;
import com.taymindis.redis.srf4j.RedisDriver;
import com.taymindis.redis.srf4j.RedisMode;
import com.taymindis.redis.srf4j.Srf4j;
import com.taymindis.redis.srf4j.intf.*;
import com.taymindis.redis.srf4j.redisearch.AggregateCommandBuilder;
import com.taymindis.redis.srf4j.redisearch.CommandBuilder;
import com.taymindis.redis.srf4j.redisearch.SearchCommandBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.taymindis.redis.srf4j.redisearch.CommandParam.*;

public class Srf4jClusteredTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // mvn clean test -Dtest=Srf4jClusteredTest#testRedisFacadeCluster
    @Test
    public void testRedisFacadeCluster() {
        try (RedisFacade facade = Srf4j.useLettuceCluster("localhost", 6379, false, null, null)) {
            try (Session session = facade.createSession()) {
                CommandBuilder searchCommandBuilder = new SearchCommandBuilder("idx/retailer/cust")
                        .query("@firstName:Dale")
                        .addOpt(WITHSCORES)
                        .addOpt(FILTER, "custId", "1", "3000")
//                        .addOpt(NOCONTENT)
                        ;
                SearchResult<String, String> res = session
                        .queryToSearchResult(searchCommandBuilder);

                if (!res.isEmpty()) {
                    System.out.println(objectMapper.writeValueAsString(res.getResult()));

                    for (int i = 0, sz = res.size(); i < sz; i++) {
                        Document<String, String> doc = (Document<String, String>) res.get(i);
//                        doc.getScore()
                        Assertions.assertNotNull(doc.getScore());
                        Assertions.assertTrue(doc.get("firstName").startsWith("Dale") || doc.get("firstName").startsWith("Ra"));
                    }
                }

                searchCommandBuilder.addOpt(NOCONTENT);
                res = session
                        .queryToSearchResult(searchCommandBuilder);

                if (!res.isEmpty()) {
                    System.out.println(objectMapper.writeValueAsString(res.getResult()));

                    for (int i = 0, sz = res.size(); i < sz; i++) {
                        Document<String, String> doc = (Document<String, String>) res.get(i);
                        Assertions.assertNull(doc.get("firstName"));
                    }
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testRedisFacadeClusterMultipleKeyPrefix() {

        Srf4j.Builder builder = Srf4j.createBuilder();

        builder.withDriver(RedisDriver.LETTUCE)
                .withRedisMode(RedisMode.CLUSTER)
                .withPooling(true).withLoadConfig(true)
                .withConfigPath("index_query.yaml");

        try (RedisFacade facade = builder.build()) {
            try (Session session = facade.createSession()) {
                CommandBuilder searchCommandBuilder = new SearchCommandBuilder("idx/retailer/custOrderss")
//                        .query("@custId:[(10 +inf]")
                        .query("@custId:10")
                        .addOpt(WITHSCORES)
//                        .addOpt(FILTER, "custId", "1", "3000")
//                        .addOpt(NOCONTENT)
                        ;

                searchCommandBuilder.addOpt(NOCONTENT);
                searchCommandBuilder.addOpt(LIMIT, "10", "6000");
                SearchResult<String, String> res = session
                        .queryToSearchResult(searchCommandBuilder);

                if (!res.isEmpty()) {
//                    System.out.println(objectMapper.writeValueAsString(res.getResult()));

                    for (int i = 0, sz = res.size(); i < sz; i++) {
                        Document<String, String> doc = (Document<String, String>) res.get(i);
                        System.out.println(doc.getId());
                    }
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testRedisFacadeClusterAggregate() {
        try (RedisFacade facade = Srf4j.useLettuceCluster("localhost", 6379, true, null, null)) {
            Thread.sleep(2000);
            for (int i = 0; i < 1000; i++) {
                if (i % 100 == 0) {
                    System.out.println("==========" + i + "==============");
                }
                try (Session session = facade.createSession()) {
                    CommandBuilder searchCommandBuilder =
                            new AggregateCommandBuilder("idx/retailer/custOrderss")
                                    .query("@custId:(1|2|3|4)")
//                                .addOpt(LOAD, "1", "@firstName")
                                    .addOpt(GROUPBY, "1", "@custId")
                                    .addOpt(REDUCE, FIRST_VALUE, "4 @firstName by @firstName asc as name")
                                    .addOpt(REDUCE, TOLIST, "1 @orderId as orderIds")
                                    .addOpt(REDUCE, TOLIST, "1 @comments as comments");
                    List<Map<String, Object>> res = session.queryToAggregateResult(searchCommandBuilder);
                    boolean gotCommentGood = false;

                    if (!Objects.requireNonNull(res).isEmpty()) {
//                    System.out.println(objectMapper.writeValueAsString(res.getResult()));
                        for (Map<String, Object> doc : res) {
//                            System.out.println(doc);
                            Object comments = doc.get("comments");
                            if (comments instanceof List) {
                                if (!gotCommentGood)
                                    gotCommentGood = ((ArrayList<String>) comments).get(0).equals("good");
                            }
                        }
                    }
                    Assertions.assertTrue(gotCommentGood, "No Comment Good found");


                    /** THIS IS Raw String Result **/
//                   List<String> rss = session.executeRawCommand(searchCommandBuilder);
//                    boolean gotCommentGood = false;
//                    for (String rs :
//                            rss) {
//                       gotCommentGood = rs.contains("good");
//                    }
//                    Assertions.assertTrue(gotCommentGood, "No Comment Good found");
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testAggregateWithBuilder() {
        Srf4j.Builder builder = Srf4j.createBuilder();

        builder.withDriver(RedisDriver.LETTUCE)
                .withRedisMode(RedisMode.CLUSTER)
                .withPooling(true).withLoadConfig(false)
                .withConfigPath("index_query.yaml");

        try (RedisFacade facade = builder.build()) {
            Thread.sleep(2000);
            for (int i = 0; i < 1000; i++) {
                if (i % 100 == 0) {
                    System.out.println("==========" + i + "==============");
                }
                try (Session session = facade.createSession()) {
                    CommandBuilder searchCommandBuilder =
                            new AggregateCommandBuilder("idx/retailer/custOrderss")
                                    .query("@custId:(1|2|3|4)")
//                                .addOpt(LOAD, "1", "@firstName")
                                    .addOpt(GROUPBY, "1", "@custId")
                                    .addOpt(REDUCE, FIRST_VALUE, "4 @firstName by @firstName asc as name")
                                    .addOpt(REDUCE, TOLIST, "1 @orderId as orderIds")
                                    .addOpt(REDUCE, TOLIST, "1 @comments as comments");
                    List<Map<String, Object>> res = session.queryToAggregateResult(searchCommandBuilder);
                    boolean gotCommentGood = false;

                    if (!Objects.requireNonNull(res).isEmpty()) {
//                    System.out.println(objectMapper.writeValueAsString(res.getResult()));
                        for (Map<String, Object> doc : res) {
//                            System.out.println(doc);
                            Object comments = doc.get("comments");
                            if (comments instanceof List) {
                                if (!gotCommentGood)
                                    gotCommentGood = ((ArrayList<String>) comments).get(0).equals("good");
                            }
                        }

                    }
                    Assertions.assertTrue(gotCommentGood, "No Comment Good found");


                    /** THIS IS Raw String Result **/
//                   List<String> rss = session.executeRawCommand(searchCommandBuilder);
//                    boolean gotCommentGood = false;
//                    for (String rs :
//                            rss) {
//                       gotCommentGood = rs.contains("good");
//                    }
//                    Assertions.assertTrue(gotCommentGood, "No Comment Good found");
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testAggregateAndSearchWithBuilder() {
        Srf4j.Builder builder = Srf4j.createBuilder();

        builder.withDriver(RedisDriver.LETTUCE)
                .withRedisMode(RedisMode.CLUSTER)
                .withPooling(true).withLoadConfig(false)
                .withConfigPath("index_query.yaml");

        try (RedisFacade facade = builder.build()) {
            Thread.sleep(2000);
            try (Session session = facade.createSession()) {
                CommandBuilder searchCommandBuilder =
                        new AggregateCommandBuilder("idx/retailer/custOrderss")
//                                .query("@custId:{1|2|3|4}")
                                .query("*")
//                                .addOpt(LOAD, "1", "@firstName")
                                .addOpt(GROUPBY, "1", "@custId")
                                .addOpt(REDUCE, FIRST_VALUE, "4 @firstName by @firstName asc as name")
                                .addOpt(REDUCE, TOLIST, "1 @orderId as orderIds")
                                .addOpt(REDUCE, TOLIST, "1 @comments as comments");
                List<Map<String, Object>> res = session.queryToAggregateResult(searchCommandBuilder);

                if (!Objects.requireNonNull(res).isEmpty()) {
                    for (Map<String, Object> rs : res) {
                        System.out.printf("Customer %s has ordered ===== \n", rs.get("name"));

                        List<String> orderIds = (List<String>) rs.get("orderIds");

                        SearchResult<String, String> subRs =
                                session.queryToSearchResult(new SearchCommandBuilder("idx/retailer/orders")
                                        .query(String.format("@orderId:{%s}", String.join("|", orderIds)))
                                        .addOpt(RETURN, "3", "orderId", "status", "orderDate"));

                        if (!subRs.isEmpty()) {
                            for (int j = 0, subsz = subRs.size(); j < subsz; j++) {
                                System.out.println((j + 1) + ". orderId = " + subRs.get(j, "orderId"));
                                System.out.println("status = " + subRs.get(j, "status"));
                                System.out.println("orderDate = " + new Date(
                                        Long.parseLong(subRs.get(j, "orderDate"))));
                            }
                        }

                        System.out.println("=============================================");

                    }
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testPublishAndSubscription() {
        Srf4j.Builder builder = Srf4j.createBuilder();

        builder.withDriver(RedisDriver.LETTUCE)
                .withRedisMode(RedisMode.CLUSTER)
                .withPooling(false).withLoadConfig(false);

        try (RedisFacade facade = builder.build()) {

            CountDownLatch countDownLatch = new CountDownLatch(2);
            AtomicBoolean atomicBoolean = new AtomicBoolean();

            PubSubSession<String, String> session = facade.createPubSubSession(new PubSubEvent<String, String>() {
                @Override
                public void message(String key, String value) {
                    atomicBoolean.compareAndSet(false, key.equals("TEST_CHANNEL") && value.equals("HELLO WORLD"));
                    countDownLatch.countDown();

                }
            });

            session.subscribe("TEST_CHANNEL");
            // WILD CARD Subs
            session.psubscribe("*_CHANNEL");

            session.publish("TEST_CHANNEL", "HELLO WORLD");

            countDownLatch.await();

            Assertions.assertTrue(atomicBoolean.get());


        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

}
