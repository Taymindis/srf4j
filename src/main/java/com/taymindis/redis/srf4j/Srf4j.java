package com.taymindis.redis.srf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.taymindis.redis.srf4j.impl.lettuce.*;
import com.taymindis.redis.srf4j.intf.RedisFacade;
import com.taymindis.redis.srf4j.intf.Session;
import com.taymindis.redis.srf4j.redisearch.*;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.RedisProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.taymindis.redis.srf4j.redisearch.CommandParam.HASH;
import static com.taymindis.redis.srf4j.redisearch.CommandParam.lookUp;

public class Srf4j {
    private static final Logger LOGGER = LoggerFactory.getLogger(Srf4j.class.getName());
    private static final int DEFAULT_PORT = 6379;
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_MASTER_ID = "mymaster";

    public static Srf4j.Builder createBuilder() {
        return new Srf4j.Builder();
    }

    public static class Builder {
        private String host;
        private int port;
        private RedisDriver driver;
        private RedisMode redisMode;

        // LETTUCE DRIVER
        private boolean usePooling;


        private String configPath;
        private boolean loadConfig;


        // FOR SENTINEL Config
        private List<String> sentinelUris;
        private String masterId;

        // FOR ACL AUTH
        private String userName;
        private CharSequence password;


        // TODO NO SSL Implement? I would prefer proxy to redis

        private Builder() {
            this.port = DEFAULT_PORT;
            this.host = DEFAULT_HOST;
            this.driver = RedisDriver.LETTUCE;
            this.redisMode = RedisMode.SINGLE;
            this.usePooling = false;
            this.configPath = "classpath:index_query.yaml";
            this.loadConfig = true;
            this.sentinelUris = null;
            this.masterId = DEFAULT_MASTER_ID;
            this.userName = null;
            this.password = null;
        }

        public Srf4j.Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Srf4j.Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Srf4j.Builder withDriver(RedisDriver driver) {
            this.driver = driver;
            return this;
        }

        public Srf4j.Builder withRedisMode(RedisMode redisMode) {
            this.redisMode = redisMode;
            return this;
        }

        public Srf4j.Builder withPooling(boolean usePooling) {
            this.usePooling = usePooling;
            return this;
        }

        public Srf4j.Builder withConfigPath(String configPath) {
            this.configPath = configPath;
            return this;
        }

        public Srf4j.Builder withLoadConfig(boolean loadConfig) {
            this.loadConfig = loadConfig;
            return this;
        }

        public Srf4j.Builder withSentinelUris(List<String> sentinelUris) {
            this.sentinelUris = sentinelUris;
            this.redisMode = RedisMode.SENTINEL;
            return this;
        }

        public Srf4j.Builder withMasterId(String masterId) {
            this.masterId = masterId;
            return this;
        }

        public Srf4j.Builder withAuth(String username, CharSequence password) {
            this.userName = username;
            this.password = password;
            return this;
        }

        public RedisFacade build() {
            switch (driver) {
                case LETTUCE:
                    final RedisFacade facade;
                    switch (redisMode) {
                        case CLUSTER:
                            facade = useLettuceCluster(this.host, this.port, this.usePooling, this.userName, this.password);
                            break;
                        case SENTINEL:
                            if (sentinelUris == null) {
                                throw new IllegalArgumentException("List of sentinelUris need to be presented");
                            }
                            facade = useLettuceSentinel(this.sentinelUris, this.masterId, this.usePooling, this.userName, this.password);
                            break;
                        case SINGLE:
                            facade = useLettuce(this.host, this.port, this.usePooling, this.userName, this.password);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected Redis Mode: " + redisMode);
                    }

                    if (this.loadConfig) {
                        tryConfigLettuceSearchFromDB(facade, configPath);
                    }
                    return facade;
                case REDISSON:
                case JEDIS:
                default:
                    throw new UnsupportedOperationException("Current is not support at your version");
            }
        }
    }


    public static RedisFacade useLettuce(String host, int port, boolean usePooling, String userName, CharSequence password) {
        RedisURI.Builder builder = RedisURI.Builder.redis(host, port);

        if (Objects.nonNull(userName) && Objects.nonNull(password)) {
            builder.withAuthentication(userName, password);
        }

        RedisURI redisUri = builder.build();
        return usePooling ? new LettuFacadePoolingImpl(redisUri) : new LettuFacadeImpl(redisUri);
    }

    public static RedisFacade useLettuceCluster(String host, int port, boolean usePooling, String userName, CharSequence password) {

        RedisURI.Builder builder = RedisURI.Builder.redis(host, port);

        if (Objects.nonNull(userName) && Objects.nonNull(password)) {
            builder.withAuthentication(userName, password);
        }

        RedisURI redisUri = builder.build();
        return usePooling ? new LettuClusterFacadePoolingImpl(redisUri) : new LettuClusterFacadeImpl(redisUri);
    }


    /*
     * Sentinels is a monitor of master health, it will delegate to failed master role failover
     * to other slave to become master, so that whoever using this facade will not be down until
     * all master&slave down
     *
     * @param sentinelUris sentinels host:port
     * @param masterId     master id among all replicas
     * @return a facade to create session
     */
    public static RedisFacade useLettuceSentinel(List<String> sentinelUris, String masterId, boolean usePooling, String userName, CharSequence password) {
        return usePooling ? new LettuSentMasterFacadePoolingImpl(sentinelUris, masterId, userName, password)
                : new LettuSentMasterFacadeImpl(sentinelUris, masterId, userName, password);
    }


    public static void tryConfigLettuceSearchFromDB(RedisFacade redisFacade, String configPath) {
        if (!(redisFacade instanceof LettuFacadeImpl ||
                redisFacade instanceof LettuSentMasterFacadeImpl ||
                redisFacade instanceof LettuClusterFacadeImpl)) {
            throw new IllegalArgumentException("client is not using lettuce driver");
        }
        try {
            File file = getFile(configPath);
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

            mapper.findAndRegisterModules();
            IndexCfg indexCfg = mapper.readValue(file, IndexCfg.class);

            if (!indexCfg.isAutoload()) {
                throw new IllegalStateException("Index is pending to load");
            }

            String indexName = indexCfg.getIndexName();
            List<QueryIndex> queryIndexes = indexCfg.getQueries();
            Map<String, String> dbConfigs = indexCfg.getDbConfig();


            String dbUrl = dbConfigs.remove("dbUrl");
            String driver = dbConfigs.remove("driver");

            Class.forName(driver);

            Properties dbProps = new Properties();

            dbProps.putAll(dbConfigs);

            try (Connection conn = DriverManager.getConnection(
                    dbUrl, dbProps)) {
                for (QueryIndex queryIndex : queryIndexes) {
                    List<Map<String, String>> listRs = new LinkedList<Map<String, String>>();
                    Statement stmt = conn.createStatement();
                    boolean importData = queryIndex.getImportData() && queryIndex.getSql() != null;
                    List<String> idxPrefix = queryIndex.getIndexPrefix();

                    String prefixName = queryIndex.getIndexAlias();

                    if (prefixName == null) {
                        prefixName = String.join("_", idxPrefix);
                    }

                    String identityNames = queryIndex.getIdentityNames();
                    List<IndexField> indexableFields = queryIndex.getIndexableFields();
                    try (Session session = redisFacade.createSession()) {
                        SchemaBuilder schemaBuilder = new SchemaBuilder();
                        if (importData) {
                            try (ResultSet rs = stmt.executeQuery(queryIndex.getSql())) {
                                boolean addedIndex = false;
                                ResultSetMetaData rsmd = rs.getMetaData();
                                int columnCount = rsmd.getColumnCount();

//                            List<Field> fields = new LinkedList<>();
                                // .addTextField("firstname", 1.0).addTextField("lastname", 1.0)
                                // .addNumericField("transdate");
                                while (rs.next()) {
                                    Map<String, String> r = new HashMap<>();
                                    String key = indexName.concat("/").concat(prefixName).concat(":");
                                    for (String identityName : identityNames.split(",")) {
                                        Object idValue = rs.getObject(identityName.trim());
                                        key = key.concat(String.valueOf(Objects.requireNonNull(idValue)))
                                                .concat("_");
                                    }

                                    r.put("__key", key.substring(0, key.length() - 1));
                                    for (int i = 1; i <= columnCount; i++) {
                                        String name = rsmd.getColumnName(i);
                                        // Only add one time
                                        if (!addedIndex) {
                                            addIndexField(schemaBuilder, indexableFields, name);
                                        }


                                        switch (rsmd.getColumnType(i)) {
                                            case Types.BIT:
                                            case Types.TINYINT:
                                            case Types.BIGINT:
                                            case Types.NUMERIC:
                                            case Types.INTEGER:
                                            case Types.SMALLINT:
                                            case Types.DECIMAL:
                                            case Types.FLOAT:
                                            case Types.DOUBLE:
                                                r.put(name, String.valueOf(rs.getObject(i)));
                                                break;
                                            case Types.DATE:
                                            case Types.TIME:
                                            case Types.TIMESTAMP:
                                                Date d = rs.getDate(i);
                                                r.put(name,
                                                        d != null ? String.valueOf(d.getTime()) : "");
                                                break;
                                            case Types.CHAR:
                                            case Types.VARCHAR:
                                            case Types.LONGVARCHAR:
                                            case Types.NCHAR:
                                            case Types.NVARCHAR:
                                            case Types.LONGNVARCHAR:
                                                r.put(name, rs.getString(i));
                                                break;
                                            case Types.LONGVARBINARY:
                                            case Types.VARBINARY:
                                            case Types.BINARY:
                                            case Types.NULL:
                                            case Types.REAL:
                                            case Types.OTHER:
                                            default:
                                                LOGGER.warn("field " + name
                                                        + " is not supporting for index search");
                                                break;
                                        }
                                    }
                                    addedIndex = true;

                                    listRs.add(r);
                                }
                            }
                        } else {
                            addIndexField(schemaBuilder, indexableFields);
                        }

                        String indexFt = "idx/".concat(indexName.concat("/").concat(prefixName));
                        CreateIdxCommandBuilder createIdxCommandBuilder = new CreateIdxCommandBuilder(indexFt);
                        createIdxCommandBuilder.setDataType(HASH);

                        for (String prefix : idxPrefix) {
                            createIdxCommandBuilder.addPrefixKey(indexName.concat("/").concat(prefix).concat(":"));
                        }
                        createIdxCommandBuilder.setSchema(schemaBuilder);
                        String output;
                        try {
//                                CustomCommandBuilder customCommandBuilder = new CustomCommandBuilder(SR4JCommandTypeExt.FT_CREATE);
                            output = session.executeForStatus(createIdxCommandBuilder);
                            if (!isSuccess(output)) {
                                throw new RedisCommandExecutionException(output);
                            }
                        } catch (ExecutionException e) {
                            if (e.getMessage().contains("Index already exists")) {
                                LOGGER.warn("Index Existed, replaced with new index");

                                output = session.executeForStatus(new CustomCommandBuilder(CommandTypeExt.FT_DROPINDEX)
                                        .addIdxOrQuery(indexFt));
                                if (!isSuccess(output)) {
                                    throw new RedisCommandExecutionException(output);
                                }
                                output = session.executeForStatus(createIdxCommandBuilder);
                                if (!isSuccess(output)) {
                                    throw new RedisCommandExecutionException(output);
                                }
                            } else {
                                LOGGER.error(e.getMessage(), e);
                            }
                        }

                    }

                    if (importData) {
                        try (Session session = redisFacade.createSession()) {
                            if (redisFacade instanceof LettuClusterFacadeImpl) {
                                session.batchExecPipeline(listRs);
                            } else {
                                session.batchExecTransaction(listRs);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

    }

    private static void addIndexField(SchemaBuilder schemaBuilder, List<IndexField> indexableFields, String name) {

        Optional<IndexField> foundField = indexableFields.stream().filter(f -> name.equalsIgnoreCase(f.getName())).findFirst();

        if (foundField.isPresent()) {
            IndexField f = foundField.get();
            if (!CommandParam.contains(f.getType())) {
                throw new RedisProtocolException("Invalid data Type given to while setting up redis data import");
            }
            Set<CommandParam> optionsCommandParam = new HashSet<>();
            if (f.getOpts() != null) {
                for (String opt :
                        f.getOpts()) {
                    CommandParam optCommandParam = CommandParam.lookUp(opt);
                    if (optCommandParam == null) {
                        throw new RedisProtocolException("Invalid field option given to while setting up redis data import");
                    }
                    optionsCommandParam.add(optCommandParam);
                }
            }
            schemaBuilder.addField(f.getName(), lookUp(f.getType()), optionsCommandParam.toArray(new CommandParam[0]));
        }
    }

    private static void addIndexField(SchemaBuilder schemaBuilder, List<IndexField> indexableFields) {
        for (IndexField f : indexableFields) {
            if (!CommandParam.contains(f.getType())) {
                throw new RedisProtocolException("Invalid data Type given to while setting up redis data import");
            }
            Set<CommandParam> optionsCommandParam = new HashSet<>();
            if (f.getOpts() != null) {
                for (String opt :
                        f.getOpts()) {
                    CommandParam optCommandParam = CommandParam.lookUp(opt);
                    if (optCommandParam == null) {
                        throw new RedisProtocolException("Invalid field option given to while setting up redis data import");
                    }
                    optionsCommandParam.add(optCommandParam);
                }
            }
            schemaBuilder.addField(f.getName(), lookUp(f.getType()), optionsCommandParam.toArray(new CommandParam[0]));
        }
    }

    private static boolean hasError(List<String> output) {
        return output == null || output.isEmpty() || "(error)".equals(output.get(0));
    }

    private static boolean isSuccess(String output) {
        return "OK".equals(output);
    }

    private static File getFile(String path) throws URISyntaxException {
        if (path.startsWith("classpath:")) {
            path = path.substring("classpath:".length());
            return new File(Objects.requireNonNull(Srf4j.class.getClassLoader().getResource(path)).getPath());
        } else {
            return new File(new URI(path));
        }
    }

}
