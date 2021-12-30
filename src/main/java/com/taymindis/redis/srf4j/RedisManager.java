//package com.taymindis.redis.srf4j;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
//import com.taymindis.redis.srf4j.impl.lettuce.*;
//import com.taymindis.redis.srf4j.intf.RedisFacade;
//import com.taymindis.redis.srf4j.intf.Session;
//import com.taymindis.redis.srf4j.redisearch.CommandParam;
//import com.taymindis.redis.srf4j.redisearch.CreateIdxCommandBuilder;
//import com.taymindis.redis.srf4j.redisearch.CustomCommandBuilder;
//import com.taymindis.redis.srf4j.redisearch.SchemaBuilder;
//import io.lettuce.core.RedisCommandExecutionException;
//import io.lettuce.core.RedisURI;
//import io.lettuce.core.protocol.RedisProtocolException;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import java.io.File;
//import java.sql.*;
//import java.sql.Date;
//import java.util.*;
//import java.util.concurrent.ExecutionException;
//import java.util.stream.Collectors;
//
//import static com.taymindis.redis.srf4j.redisearch.CommandParam.*;
//
//public class RedisManager {
//    private static final Logger LOGGER = LogManager.getLogger(RedisManager.class.getName());
//    private static final int DEFAULT_PORT = 6379;
//
//    public static RedisFacade useLettuce(String host, int port, boolean loadConfig, boolean usePooling) {
//        RedisURI redisUri = RedisURI.Builder.redis(host, port).build();
//        RedisFacade redisFacade = usePooling ? new LettuFacadePoolingImpl(redisUri) : new LettuFacadeImpl(redisUri);
//        if (loadConfig) {
//            tryConfigLettuceSearchFromDB(redisFacade);
//        }
//        return redisFacade;
//    }
//
//    public static RedisFacade useLettuce(String host, int port, boolean loadConfig) {
//        return useLettuce(host, port, loadConfig, false);
//    }
//
//    public static RedisFacade useLettuce(String host, int port) {
//        return useLettuce(host, port, true);
//    }
//
//    public static RedisFacade useLettuce(String host) {
//        return useLettuce(host, DEFAULT_PORT);
//    }
//
//    public static RedisFacade useLettuceCluster(String host, int port, boolean loadConfig, boolean usePooling) {
//        RedisURI redisUri = RedisURI.Builder.redis(host, port).build();
//        RedisFacade redisFacade = usePooling ? new LettuClusterFacadePoolingImpl(redisUri) : new LettuClusterFacadeImpl(redisUri);
//        if (loadConfig) {
//            tryConfigLettuceSearchFromDB(redisFacade);
//        }
//        return redisFacade;
//    }
//
//    public static RedisFacade useLettuceCluster(String host, int port, boolean loadConfig) {
//        return useLettuceCluster(host, port, loadConfig, false);
//    }
//
//    public static RedisFacade useLettuceCluster(String host, int port) {
//        return useLettuceCluster(host, port, true);
//    }
//
//    public static RedisFacade useLettuceCluster(String host) {
//        return useLettuceCluster(host, DEFAULT_PORT);
//    }
//
//    /**
//     *
//     * @param clusteredUris clustered host:port uri
//     * @param clusterId clustered master id
//     * @return a facade to create session
//     */
////    public static RedisFacade useLettuceCluster(List<String> clusteredUris, String clusterId) {
////
//////        List<RedisURI> redisURIS = hostPorts.stream().map(host -> {
//////            String[] hostPort = host.split(":");
//////            return RedisURI.Builder.redis(hostPort[0],
//////                    hostPort.length==1?6379:Integer.parseInt(hostPort[1])).build();
//////
//////        }).collect(Collectors.toList());
////
////        List<RedisURI> sentinelUris = clusteredUris.stream().map(host -> {
////            String[] hostPort = host.split(":");
////            return RedisURI.Builder.redis(hostPort[0],
////                    hostPort.length==1?26379:Integer.parseInt(hostPort[1])).build();
////
////        }).collect(Collectors.toList());
////
////        RedisFacade redisFacade = new LettuClusterFacadeImpl(clusteredUris, clusterId);
////        tryConfigLettuceSearchFromDB(redisFacade);
////        return redisFacade;
////
////    }
//
//    /**
//     * Sentinels is a monitor of master health, it will delegate to failed master role failover
//     * to other slave to become master, so that whoever using this facade will not be down until
//     * all master&slave down
//     *
//     * @param sentinelUris sentinels host:port
//     * @param clusterId    clustered master id
//     * @return a facade to create session
//     */
//    public static RedisFacade useLettuceSentinel(List<String> sentinelUris, String clusterId, boolean loadConfig, boolean usePooling) {
//        RedisFacade redisFacade = usePooling ? new LettuSentMasterFacadePoolingImpl(sentinelUris, clusterId )
//                : new LettuSentMasterFacadeImpl(sentinelUris, clusterId);
//        if (loadConfig) {
//            tryConfigLettuceSearchFromDB(redisFacade);
//        }
//        return redisFacade;
//    }
//
//    public static RedisFacade useLettuceSentinel(List<String> sentinelUris, String clusterId, boolean loadConfig) {
//        return useLettuceSentinel(sentinelUris, clusterId, loadConfig, false);
//    }
//
//    public static RedisFacade useLettuceSentinel(List<String> sentinelUris, String clusterId) {
//        return useLettuceSentinel(sentinelUris, clusterId, true);
//
//    }
//
//
//    public static void tryConfigLettuceSearchFromDB(RedisFacade redisFacade) {
//        if (!(redisFacade instanceof LettuFacadeImpl ||
//                redisFacade instanceof LettuSentMasterFacadeImpl ||
//                redisFacade instanceof LettuClusterFacadeImpl)) {
//            throw new IllegalArgumentException("client is not using lettuce driver");
//        }
//        try {
//            File file = new File(RedisManager.class.getClassLoader().getResource("index_query.yaml").getPath());
//            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
//
//            mapper.findAndRegisterModules();
//            IndexCfg indexCfg = mapper.readValue(file, IndexCfg.class);
//
//            if (!indexCfg.isAutoload()) {
//                throw new IllegalStateException("Index is pending to load");
//            }
//
//            String indexName = indexCfg.getIndexName();
//            List<QueryIndex> queryIndexes = indexCfg.getQueries();
//            Map<String, String> dbConfigs = indexCfg.getDbConfig();
//
//            String dbUrl = dbConfigs.remove("dbUrl");
//            String driver = dbConfigs.remove("driver");
//
//            Class.forName(driver);
//
//            Properties dbProps = new Properties();
//
//            dbProps.putAll(dbConfigs);
//
//            try (Connection conn = DriverManager.getConnection(
//                    dbUrl, dbProps)) {
//                for (QueryIndex queryIndex : queryIndexes) {
//                    List<Map<String, String>> listRs = new LinkedList<Map<String, String>>();
//                    Statement stmt = conn.createStatement();
//                    Boolean importData = queryIndex.getImportData();
//                    try (ResultSet rs = stmt.executeQuery(queryIndex.getSql())) {
//                        ResultSetMetaData rsmd = rs.getMetaData();
//                        int columnCount = rsmd.getColumnCount();
//                        String idxPref = queryIndex.getIndexPrefix();
//                        String identityNames = queryIndex.getIdentityNames();
//                        List<IndexField> indexableFields = queryIndex.getIndexableFields();
//                        try (Session session = redisFacade.createSession()) {
//                            SchemaBuilder schemaBuilder = new SchemaBuilder();
////                            List<Field> fields = new LinkedList<>();
//                            // .addTextField("firstname", 1.0).addTextField("lastname", 1.0)
//                            // .addNumericField("transdate");
//                            while (rs.next()) {
//                                Map<String, String> r = new HashMap<>();
//                                String key = indexName.concat("/").concat(idxPref).concat(":");
//                                for (String identityName : identityNames.split(",")) {
//                                    Object idValue = rs.getObject(identityName.trim());
//                                    key = key.concat(String.valueOf(Objects.requireNonNull(idValue)))
//                                            .concat("_");
//                                }
//
//                                r.put("__key", key.substring(0, key.length() - 1));
//                                for (int i = 1; i <= columnCount; i++) {
//                                    String name = rsmd.getColumnName(i);
//
//                                    // Only add one time
//                                    if (listRs.isEmpty()) {
//                                        addIndexField(schemaBuilder, indexableFields, name);
//                                    }
//
//
//                                    switch (rsmd.getColumnType(i)) {
//                                        case Types.BIT:
//                                        case Types.TINYINT:
//                                        case Types.BIGINT:
//                                        case Types.NUMERIC:
//                                        case Types.INTEGER:
//                                        case Types.SMALLINT:
//                                        case Types.DECIMAL:
//                                        case Types.FLOAT:
//                                        case Types.DOUBLE:
//                                            r.put(name, String.valueOf(rs.getObject(i)));
//                                            break;
//                                        case Types.DATE:
//                                        case Types.TIME:
//                                        case Types.TIMESTAMP:
//                                            Date d = rs.getDate(i);
//                                            r.put(name,
//                                                    d != null ? String.valueOf(d.getTime()) : "");
//                                            break;
//                                        case Types.CHAR:
//                                        case Types.VARCHAR:
//                                        case Types.LONGVARCHAR:
//                                        case Types.NCHAR:
//                                        case Types.NVARCHAR:
//                                        case Types.LONGNVARCHAR:
//                                            r.put(name, rs.getString(i));
//                                            break;
//                                        case Types.LONGVARBINARY:
//                                        case Types.VARBINARY:
//                                        case Types.BINARY:
//                                        case Types.NULL:
//                                        case Types.REAL:
//                                        case Types.OTHER:
//                                        default:
//                                            LOGGER.warn("field " + name
//                                                    + " is not supporting for index search");
//                                            break;
//                                    }
//                                }
//                                if (!importData)
//                                    break;
//                                listRs.add(r);
//                            }
//
//
//                            String indexFt = "idx/".concat(indexName.concat("/").concat(idxPref));
//                            CreateIdxCommandBuilder createIdxCommandBuilder = new CreateIdxCommandBuilder(indexFt);
//                            createIdxCommandBuilder.setDataType(HASH);
//
//                            for (String prefix : idxPref.split("_")) {
//                                createIdxCommandBuilder.addPrefixKey(indexName.concat("/").concat(prefix).concat(":"));
//                            }
//                            createIdxCommandBuilder.setSchema(schemaBuilder);
//                            String output;
//                            try {
//
////                                CustomCommandBuilder customCommandBuilder = new CustomCommandBuilder(SR4JCommandTypeExt.FT_CREATE);
//                                output = session.executeForStatus(createIdxCommandBuilder);
//                                if (!isSuccess(output)) {
//                                    throw new RedisCommandExecutionException(output);
//                                }
//                            } catch (ExecutionException e) {
//                                if (e.getMessage().contains("Index already exists")) {
//                                    LOGGER.warn("Index Existed, replaced with new index");
//
//                                    output = session.executeForStatus(new CustomCommandBuilder(SR4JCommandTypeExt.FT_DROPINDEX)
//                                            .addIdxOrQuery(indexFt));
//                                    if (!isSuccess(output)) {
//                                        throw new RedisCommandExecutionException(output);
//                                    }
//                                    output = session.executeForStatus(createIdxCommandBuilder);
//                                    if (!isSuccess(output)) {
//                                        throw new RedisCommandExecutionException(output);
//                                    }
//                                } else {
//                                    LOGGER.error(e.getMessage(), e);
//                                }
//                            }
//                        }
//                    }
//
//                    if (importData) {
//                        try (Session session = redisFacade.createSession()) {
//                            if (redisFacade instanceof LettuClusterFacadeImpl) {
//                                session.batchExecPipeline(listRs);
//                            } else {
//                                session.batchExecTransaction(listRs);
//                            }
//                        }
//                    }
//                }
//            } catch (Exception e) {
//                LOGGER.error(e.getMessage(), e);
//            }
//
//        } catch (Exception e) {
//            LOGGER.error(e.getMessage(), e);
//        }
//
//    }
//
//    private static void addIndexField(SchemaBuilder schemaBuilder, List<IndexField> indexableFields, String name) {
//
//        List<IndexField> foundFields = indexableFields.stream().filter(f -> name.equals(f.getName())).collect(Collectors.toList());
//
//        for (IndexField f :
//                foundFields) {
//            if (!CommandParam.contains(f.getType())) {
//                throw new RedisProtocolException("Invalid data Type given to while setting up redis data import");
//            }
//            Set<CommandParam> optionsCommandParam = new HashSet<>();
//            if (f.getOpts() != null) {
//                for (String opt :
//                        f.getOpts()) {
//                    CommandParam optCommandParam = CommandParam.lookUp(opt);
//                    if (optCommandParam == null) {
//                        throw new RedisProtocolException("Invalid field option given to while setting up redis data import");
//                    }
//                    optionsCommandParam.add(optCommandParam);
//                }
//            }
//
//            schemaBuilder.addField(name, lookUp(f.getType()), optionsCommandParam.toArray(new CommandParam[0]));
//            break;
//        }
//
//    }
//
//    private static boolean hasError(List<String> output) {
//        return output == null || output.isEmpty() || "(error)".equals(output.get(0));
//    }
//
//    private static boolean isSuccess(String output) {
//        return "OK".equals(output);
//    }
//
//}
