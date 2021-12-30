package com.taymindis.redis.srf4j.redisearch;

import java.util.*;

import static com.taymindis.redis.srf4j.redisearch.CommandParam.*;

public class CreateIdxCommandBuilder extends CommandBuilder{

    private CommandTypeExt commandTypeExt = CommandTypeExt.FT_CREATE;
    private final String idxName;
    private CommandParam dataType = HASH;
    private List<CharSequence> prefixKeys = new ArrayList<>();
    private String preFiltered = null;
    private String language, languageField = null;
    private String score, scoreField = null;
    private String payloadField = null;
    private String maxExtFields = null;
    private List<String> stopwords = null;

    /**
     * SCHEMA {identifier} [AS {attribute}]
     * [TEXT [NOSTEM] [WEIGHT {weight}] [PHONETIC {matcher}] | NUMERIC | GEO | TAG [SEPARATOR {sep}]
     * [CASESENSITIVE] [SORTABLE [UNF]] [NOINDEX]]
     */
    private SchemaBuilder schema;

    public CreateIdxCommandBuilder(String idxName) {
        this.idxName = idxName;
    }

    public String getIdxName() {
        return idxName;
    }

    public CommandTypeExt getSr4JCommandTypeExt() {
        return commandTypeExt;
    }

    public CommandParam getDataType() {
        return dataType;
    }

    public CreateIdxCommandBuilder setDataType(CommandParam dataType) {
        this.dataType = dataType;
        return this;
    }

    public List<CharSequence> getPrefixKeys() {
        return prefixKeys;
    }

    public CreateIdxCommandBuilder addPrefixKey(String prefixKeys) {
        this.prefixKeys.add(prefixKeys);
        return this;
    }

    public String getPreFiltered() {
        return preFiltered;
    }

    public CreateIdxCommandBuilder setPreFiltered(String preFiltered) {
        this.preFiltered = preFiltered;
        return this;
    }

    public String getLanguage() {
        return language;
    }

    public CreateIdxCommandBuilder setLanguage(String language) {
        this.language = language;
        return this;
    }

    public String getLanguageField() {
        return languageField;
    }

    public CreateIdxCommandBuilder setLanguageField(String languageField) {
        this.languageField = languageField;
        return this;
    }

    public String getScore() {
        return score;
    }

    public CreateIdxCommandBuilder setScore(String score) {
        this.score = score;
        return this;
    }

    public String getScoreField() {
        return scoreField;
    }

    public CreateIdxCommandBuilder setScoreField(String scoreField) {
        this.scoreField = scoreField;
        return this;
    }

    public String getPayloadField() {
        return payloadField;
    }

    public CreateIdxCommandBuilder setPayloadField(String payloadField) {
        this.payloadField = payloadField;
        return this;
    }

    public String getMaxExtFields() {
        return maxExtFields;
    }

    public CreateIdxCommandBuilder setMaxExtFields(String maxExtFields) {
        this.maxExtFields = maxExtFields;
        return this;
    }

    public List<String> getStopwords() {
        return stopwords;
    }

    public CreateIdxCommandBuilder addStopWords(String stopword) {
        if (this.stopwords == null) {
            this.stopwords = new ArrayList<String>();
        }
        this.stopwords.add(stopword);
        return this;
    }

    public SchemaBuilder getSchema() {
        return schema;
    }

    public CreateIdxCommandBuilder setSchema(SchemaBuilder schema) {
        this.schema = schema;
        return this;
    }

    public List<CharSequence> build() {
        List<CharSequence> args = new ArrayList<CharSequence>();

        if (prefixKeys.isEmpty() || idxName == null) {
            throw new IllegalArgumentException("idxName || targetKeyPrefixes must be present");
        }

        args.add(idxName);

        args.add(ON);
        args.add(dataType.toString());


        args.add(PREFIX);
        args.add(String.valueOf(prefixKeys.size()));
        args.addAll(prefixKeys);


        if (preFiltered != null) {
            args.add(PREFIX.toString());
            args.add(preFiltered);
        }


        if (language != null) {
            args.add(LANGUAGE);
            args.add(language);
        }


        if (languageField != null) {
            args.add(LANGUAGE_FIELD.toString());
            args.add(languageField);
        }


        if (score != null) {
            args.add(SCORE.toString());
            args.add(score);
        }


        if (scoreField != null) {
            args.add(SCORE_FIELD.toString());
            args.add(scoreField);
        }


        if (payloadField != null) {
            args.add(PAYLOAD_FIELD.toString());
            args.add(payloadField);
        }


        if (maxExtFields != null) {
            args.add(MAXTEXTFIELDS.toString());
            args.add(maxExtFields);
        }


        if (maxExtFields != null) {
            args.add(MAXTEXTFIELDS.toString());
            // [MAXTEXTFIELDS] [TEMPORARY {seconds}] [NOOFFSETS] [NOHL] [NOFIELDS] [NOFREQS] [SKIPINITIALSCAN]
            args.add(maxExtFields);
        }

        if (stopwords != null) {
            args.add(STOPWORDS.toString());
            args.add(String.valueOf(stopwords.size()));
            args.addAll(stopwords);
        }


        if (schema != null) {
            args.add(SCHEMA);
            args.addAll(schema.build());
        }


        return args;
    }
}
