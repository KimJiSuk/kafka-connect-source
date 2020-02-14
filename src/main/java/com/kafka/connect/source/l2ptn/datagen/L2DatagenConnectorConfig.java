package com.kafka.connect.source.l2ptn.datagen;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class L2DatagenConnectorConfig extends AbstractConfig {
    public static final String TOPICS_CONF = "topics";
    private static final String TOPICS_DOC = "Topic to write to";
    public static final String TOPICS_WORK_CONF = "topics.work";
    private static final String TOPICS_WORK_DOC = "On/Off topic";
    public static final String FILE_NAME_PREFIX_CONF = "file.name.prefix";
    private static final String FILE_NAME_PREFIX_DOC = "FileName prefix List";
    public static final String FILE_NAME_DATEFORMAT_CONF = "file.name.dateformat";
    private static final String FILE_NAME_DATEFORMAT_DOC = "FileName dateformat";
    public static final String FILE_NAME_TEMPLATE_CONF = "file.name.template";
    private static final String FILE_NAME_TEMPLATE_DOC = "FileName Template";
    public static final String FILE_PATH_CONF = "file.path";
    private static final String FILE_PATH_DOC = "File Path";

    public L2DatagenConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
        validate();
    }

    public L2DatagenConnectorConfig(Map<String, String> parsedConfig) {
        this(configDef(), parsedConfig);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(TOPICS_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, TOPICS_DOC)
                .define(TOPICS_WORK_CONF, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, TOPICS_WORK_DOC)
                .define(FILE_NAME_PREFIX_CONF, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, FILE_NAME_PREFIX_DOC)
                .define(FILE_NAME_DATEFORMAT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_NAME_DATEFORMAT_DOC)
                .define(FILE_NAME_TEMPLATE_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_NAME_TEMPLATE_DOC)
                .define(FILE_PATH_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_PATH_DOC);
    }

    private void validate() {
        // TODO add validate
    }

    public List getTopics() { return this.getList(TOPICS_CONF); }

    public List getTopicsWork() { return this.getList(TOPICS_WORK_CONF); }

    public List getFileNamePrefix() { return this.getList(FILE_NAME_PREFIX_CONF); }

    public String getFileNameDateformat() { return this.getString(FILE_NAME_DATEFORMAT_CONF); }

    public String getFileNameTemplate() { return this.getString(FILE_NAME_TEMPLATE_CONF); }

    public String getFilePath() { return this.getString(FILE_PATH_CONF); }
}
