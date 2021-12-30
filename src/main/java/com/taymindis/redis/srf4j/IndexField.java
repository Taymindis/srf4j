package com.taymindis.redis.srf4j;


import java.util.List;

public class IndexField {
    private String name;
    private String type;
    private List<String> opts;

    public IndexField() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getOpts() {
        return opts;
    }

    public void setOpts(List<String> opts) {
        this.opts = opts;
    }
}
