package com.taymindis.redis.srf4j.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.*;
import com.taymindis.redis.srf4j.RedisMode;
import com.taymindis.redis.srf4j.RedisOpsMode;
import com.taymindis.redis.srf4j.Srf4j;
import com.taymindis.redis.srf4j.intf.*;
import com.taymindis.redis.srf4j.redisearch.AggregateCommandBuilder;
import com.taymindis.redis.srf4j.redisearch.CommandBuilder;
import com.taymindis.redis.srf4j.redisearch.CustomCommandBuilder;
import com.taymindis.redis.srf4j.redisearch.SearchCommandBuilder;
import io.lettuce.core.protocol.CommandType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BinaryOperator;

import static com.taymindis.redis.srf4j.redisearch.CommandParam.*;

public class Srf4jTest {
    private static final Logger logger = LoggerFactory.getLogger(Srf4jTest.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testRedisSearchSentinelReactive() throws Exception {
        try (RedisFacade redisFacade = Srf4j.useLettuceSentinel(
                List.of("127.0.0.1:26379",
                        "127.0.0.1:26479",
                        "127.0.0.1:26579"), "mymaster", true, null, null)) {
            try (Session session = redisFacade.createSession()) {
                CommandBuilder searchCommandBuilder =
                        new SearchCommandBuilder
                                ("idx/retailer/cust")
                                .query("@firstName:Dale").addOpt(NOCONTENT);

                SearchResult<String, String> res =
                        session.queryToSearchResult(searchCommandBuilder);


                if (!res.isEmpty()) {
                    System.out.println(objectMapper.writeValueAsString(res.getResult()));

                    for (int i = 0, sz = res.size(); i < sz; i++) {
                        Document<String, String> doc = (Document<String, String>) res.get(i);
                        Assertions.assertTrue(doc.get("firstName").startsWith("Dale") || doc.get("firstName").startsWith("Ra"));
                    }
                }
            } catch (JsonProcessingException | InterruptedException | ExecutionException jsonProcessingException) {
                jsonProcessingException.printStackTrace();
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testRedisFacadeSentinelAggregate() {
        Srf4j.Builder builder = Srf4j.createBuilder();

        builder.withSentinelUris(List.of(
                "127.0.0.1:26379",
                "127.0.0.1:26479",
                "127.0.0.1:26579"
        )).withMasterId("mymaster").withPooling(true)
                .withAuth("default", "pass")
                .withLoadConfig(true);

        try (RedisFacade facade = builder.build()) {
            Thread.sleep(1000);
            for (int i = 0; i < 100; i++) {
                if (i % 10 == 0) {
                    logger.info("=========={}==============", i);
                }
                try (Session session = facade.createSlaveSession()) {
                    CommandBuilder searchCommandBuilder =
                            new AggregateCommandBuilder("idx/retailer/custOrders")
                                    .query("@custId:{1|2|3|4}")
//                                .addOpt(LOAD, "1", "@firstName")
                                    .addOpt(GROUPBY, "1", "@custId")
                                    .addOpt(REDUCE, FIRST_VALUE, "4 @firstName by @firstName asc as name")
                                    .addOpt(REDUCE, TOLIST, "1 @orderId as orderIds")
                                    .addOpt(REDUCE, TOLIST, "1 @comments as comments");
                    List<Map<String, Object>> res = session
                            .queryToAggregateResult(searchCommandBuilder);
                    Assertions.assertTrue(!res.isEmpty());

//                    System.out.println(objectMapper.writeValueAsString(res.getResult()));
                    boolean gotCommentGood = false;
                    for (Map<String, Object> doc : res) {
                        Object comments = doc.get("comments");
                        if (!gotCommentGood && comments instanceof List) {
                            gotCommentGood = ((ArrayList<String>) comments).get(0).equals("good");
                        }
                    }

                    Assertions.assertTrue(gotCommentGood, "No Comment Good found");


                    searchCommandBuilder =  new AggregateCommandBuilder("idx/retailer/custOrders")
                            .query("@orderId:{1|1888|4|2000}")
                            .addOpt(GROUPBY, "1", "@orderId")
                            .addOpt(REDUCE, FIRST_VALUE, "4 @custId by @custId asc as firstCustId")
                            .addOpt(REDUCE, TOLIST, "1 @comments as comments")
                            .addOpt(REDUCE, TOLIST, "1 @productCode as productCodes")
                    ;


                    res = session
                            .queryToAggregateResult(searchCommandBuilder);
                    Assertions.assertFalse(res.isEmpty());
//
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testRedisFacadeSentinelCollectionAPI() {
        Srf4j.Builder builder = Srf4j.createBuilder();

        builder.withSentinelUris(List.of(
                "127.0.0.1:26379",
                "127.0.0.1:26479",
                "127.0.0.1:26579"
        )).withMasterId("mymaster").withPooling(true)
                .withRedisMode(RedisMode.SENTINEL)
//                .withAuth("default", "pass")
                .withLoadConfig(false);
        testCollectionApi(builder);
    }

    @Test
    public void testRedisFacadeSentinelCollectionAPIWithAuth() {
        Srf4j.Builder builder = Srf4j.createBuilder();

        builder.withSentinelUris(List.of(
                "127.0.0.1:26379",
                "127.0.0.1:26479",
                "127.0.0.1:26579"
        )).withMasterId("mymaster").withPooling(true)
                .withRedisMode(RedisMode.SENTINEL)
                .withAuth("default", "pass")
                .withLoadConfig(false);
        testCollectionApi(builder);
    }

    @Test
    public void usingCustomCommander() {
        Srf4j.Builder builder = Srf4j.createBuilder();

        builder.withSentinelUris(List.of(
                "127.0.0.1:26379",
                "127.0.0.1:26479",
                "127.0.0.1:26579"
        )).withMasterId("mymaster").withPooling(true)
                .withRedisMode(RedisMode.SENTINEL)
                .withAuth("default", "pass")
                .withLoadConfig(false);


        try (RedisFacade facade = builder.build()) {
            try (ListSession<String> session = facade.useListSession("tadika")) {

                session.push("ABC", "EFG");


                CustomCommandBuilder cmb = new CustomCommandBuilder(CommandType.EXPIRE)
                        .addIdxOrQuery("tadika", "1");

                session.executeRawCommand(cmb);


                Assertions.assertEquals("ABC", session.pop());

                Thread.sleep(1000);

                Assertions.assertNull(session.pop());

                System.out.println("Done");
            }


        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void testCollectionApi(Srf4j.Builder builder) {

        try (RedisFacade facade = builder.build()) {
            try (ListSession<String> session = facade.useListSession("tadika")) {

                System.out.println(session.push("ABC"));

                System.out.println(session.push("EFG", RedisOpsMode.RIGHT_IN));

                System.out.println(session.push("XYZ", RedisOpsMode.RIGHT_IN));


                System.out.println("Expire success = " + session.setExpire("tadika", 5));


                Assertions.assertEquals(session.pop(), "XYZ");

                Assertions.assertEquals(session.pop(RedisOpsMode.LEFT_OUT), "ABC");

                Assertions.assertEquals(session.pop(RedisOpsMode.RIGHT_OUT), "EFG");


                Long rs = session.push(Duration.ofSeconds(10), "1", "3", "6", "8", "10");

//                rs.forEach(System.out::println);


                Assertions.assertEquals(5, rs);

                List<String> popResult = session.pop(5);

                String rsStr = popResult.stream().reduce("", new BinaryOperator<String>() {
                    @Override
                    public String apply(String s, String s2) {
                        return s + s2 + ",";
                    }
                });
                Assertions.assertEquals("1,3,6,8,10,", rsStr);

                System.out.println("Test Done");
            }


        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

//    @Test
//    public void testRedisLettuCustomCommand() throws ExecutionException, InterruptedException {
//        RedisURI redisUri = RedisURI.Builder.redis("localhost", 6379).build();
//        RedisClient client = RedisClient.create(redisUri);
////
//        StatefulRedisConnection<String, String> connection = client.connect();
//        RedisCommand<String, String, String> command =
//                new Command<>(CommandType.PING,
//                        new StatusOutput<>(StringCodec.UTF8));
////
//        AsyncCommand<String, String, String> asyncCommand = new AsyncCommand<>(command);
//        connection.dispatch(asyncCommand);
//
//        System.out.println(asyncCommand.get());
//
//
////        System.out.println(query(FT_SEARCH, connection, "database_idx", "primary"));
//        List<String> rs = executeToRawStringList(FT_SEARCH, connection,
//                "database_idx Red* RETURN 2 title body"
//        );
//
//
//        SearchResult<String, String> rs2 = queryToSearchResult(FT_SEARCH, connection,
//                "database_idx Red* RETURN 2 title body"
//        );
//
//        System.out.println(executeToRawStringList(FT_INFO, connection, "database_idx"));
//
//        System.out.println(rs);
//
//    }


}
