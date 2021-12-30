package com.taymindis.redis.srf4j;

import java.util.List;

public class QueryIndex {
    private List<String> indexPrefix;
    private String indexAlias = null;
    private String identityNames;
    private List<IndexField> indexableFields;
    private String sql;
    private Boolean importData = true;

    public QueryIndex() {}

    public List<String> getIndexPrefix() {
        return indexPrefix;
    }

    public void setIndexPrefix(List<String> indexPrefix) {
        this.indexPrefix = indexPrefix;
    }

    public String getIndexAlias() {
        return indexAlias;
    }

    public void setIndexAlias(String indexAlias) {
        this.indexAlias = indexAlias;
    }

    public String getIdentityNames() {
        return identityNames;
    }

    public void setIdentityNames(String identityNames) {
        this.identityNames = identityNames;
    }

    public List<IndexField> getIndexableFields() {
        return indexableFields;
    }

    public void setIndexableFields(List<IndexField> indexableFields) {
        this.indexableFields = indexableFields;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Boolean getImportData() {
        return importData;
    }

    public void setImportData(Boolean importData) {
        this.importData = importData;
    }



    // Constructors, Getters, Setters and toString


}
