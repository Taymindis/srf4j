package com.taymindis.redis.srf4j.redisearch;

public enum CommandParam implements CharSequence {

    ON("ON"),
    HASH("HASH"),
    JSON("JSON"),

    PREFIX("PREFIX"),
    FILTER("FILTER"),
    LANGUAGE("LANGUAGE"),
    LANGUAGE_FIELD("LANGUAGE_FIELD"),
    SCORE("SCORE"),
    SCORE_FIELD("SCORE_FIELD"),
    PAYLOAD_FIELD("PAYLOAD_FIELD"),
    MAXTEXTFIELDS("MAXTEXTFIELDS"),
    TEMPORARY("TEMPORARY"),
    NOOFFSETS("NOOFFSETS"),
    NOFIELDS("NOFIELDS"),
    NOFREQS("NOFREQS"),
    SKIPINITIALSCAN("SKIPINITIALSCAN"),


    SCHEMA("SCHEMA"),
    STOPWORDS("STOPWORDS"),

// [TEMPORARY {seconds}] [NOOFFSETS] [NOHL] [NOFIELDS] [NOFREQS] [SKIPINITIALSCAN]


    /** Schema **/
    TEXT("TEXT"),
    NOSTEM("NOSTEM"),
    WEIGHT("WEIGHT"),
    PHONETIC("PHONETIC"),
    NUMERIC("NUMERIC"),
    GEO("GEO"),
    TAG("TAG"),
    SEPARATOR("SEPARATOR"),
    CASESENSITIVE("CASESENSITIVE"),
    SORTABLE("SORTABLE"),
    UNF("UNF"),
    NOINDEX("NOINDEX"),


    WITHSCORES("WITHSCORES"),
    NOCONTENT("NOCONTENT"),
    EXPLAINSCORE("EXPLAINSCORE"),


    RETURN("RETURN"),
    EXPANDER("EXPANDER"),
    SORTBY("SORTBY"),
    LIMIT("LIMIT"),

    /** AGGREGATE **/
    LOAD("LOAD"),
    GROUPBY("GROUPBY"),
    REDUCE("REDUCE"),
    MIN("MIN"),
    MAX("MAX"),
    AVG("AVG"),
    FIRST_VALUE("FIRST_VALUE"),
    TOLIST("TOLIST"),
    STDDEV("STDDEV"),
    QUANTILE("QUANTILE"),
    RANDOM_SAMPLE("RANDOM_SAMPLE"),
    APPLY("APPLY"),


    SET("SET"),
    GET("GET")


    /** TODO CURSOR **/

    /** END CURSOR **/


    ;



    private final String value;

    /**
     * @param v the command value
     */
    CommandParam(final String v) {
        this.value = v;
    }

    @Override
    public int length() {
        return value.length();
    }

    @Override
    public char charAt(int index) {
        return value.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return value.subSequence(start, end);
    }

    @Override
    public String toString() {
        return value;
    }

    public static boolean contains(String v) {
        for (CommandParam c : CommandParam.values()) {
            if (c.name().equals(v)) {
                return true;
            }
        }
        return false;
    }
    public static CommandParam lookUp(String v) {
        for (CommandParam c : CommandParam.values()) {
            if (c.name().equals(v)) {
                return c;
            }
        }
        return null;
    }
}
