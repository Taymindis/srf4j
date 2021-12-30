
package com.taymindis.redis.srf4j;

import java.util.List;
import java.util.Map;

public class IndexCfg {
    private boolean autoload;
    private String sqlDriver;
    private Map<String,String> dbConfig;
    private String user;
    private String password;
    private String indexName;
    private List<QueryIndex> queries;

    public IndexCfg(boolean autoload, Map<String, String> dbConfig, String user, String password, String indexName, List<QueryIndex> queries) {
        this.autoload = autoload;
        this.dbConfig = dbConfig;
        this.user = user;
        this.password = password;
        this.indexName = indexName;
        this.queries = queries;
    }

    public IndexCfg() {}

    public boolean isAutoload() {
        return autoload;
    }

    public void setAutoload(boolean autoload) {
        this.autoload = autoload;
    }

    public Map<String, String> getDbConfig() {
        return dbConfig;
    }

    public void setDbConfig(Map<String, String> dbConfig) {
        this.dbConfig = dbConfig;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<QueryIndex> getQueries() {
        return queries;
    }

    public void setQueries(List<QueryIndex> queries) {
        this.queries = queries;
    }

    public String getSqlDriver() {
        return sqlDriver;
    }

    public void setSqlDriver(String sqlDriver) {
        this.sqlDriver = sqlDriver;
    }
}
