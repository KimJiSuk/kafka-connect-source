package com.kafka.connect.source.l2ptn.ftp.pm;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class L2ParserConnectorConfig extends AbstractConfig {
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
    public static final String FTP_URL_CONF = "ftp.url";
    private static final String FTP_URL_DOC = "FTP Url";
    public static final String FTP_PORT_CONF = "ftp.port";
    private static final String FTP_PORT_DOC = "FTP Port";
    public static final String FTP_ID_CONF = "ftp.id";
    private static final String FTP_ID_DOC = "FTP ID";
    public static final String FTP_PSWD_CONF = "ftp.pswd";
    private static final String FTP_PSWD_DOC = "FTP Password";
    public static final String FTP_COLLECT_DURATION_CONF = "ftp.collect.duration";
    private static final String FTP_COLLECT_DURATION_DOC = "FTP Collect Duration";
    public static final String FTP_TIME_OFFSET_CONF = "ftp.time.offset";
    private static final String FTP_TIME_OFFSET_DOC = "FTP Time Offset";
    public static final String FTP_CONNECT_TIMEOUT_CONF = "ftp.connect.timeout";
    private static final String FTP_CONNECT_TIMEOUT_DOC = "FTP Connect Timeout";
    public static final String FTP_READ_TIMEOUT_CONF = "ftp.read.timeout";
    private static final String FTP_READ_TIMEOUT_DOC = "FTP Read Timeout";

    public L2ParserConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
        validate();
    }

    public L2ParserConnectorConfig(Map<String, String> parsedConfig) {
        this(configDef(), parsedConfig);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(TOPICS_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, TOPICS_DOC)
                .define(TOPICS_WORK_CONF, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, TOPICS_WORK_DOC)
                .define(FILE_NAME_PREFIX_CONF, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, FILE_NAME_PREFIX_DOC)
                .define(FILE_NAME_DATEFORMAT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_NAME_DATEFORMAT_DOC)
                .define(FILE_NAME_TEMPLATE_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_NAME_TEMPLATE_DOC)
                .define(FILE_PATH_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_PATH_DOC)
                .define(FTP_URL_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FTP_URL_DOC)
                .define(FTP_PORT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FTP_PORT_DOC)
                .define(FTP_ID_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FTP_ID_DOC)
                .define(FTP_PSWD_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FTP_PSWD_DOC)
                .define(FTP_COLLECT_DURATION_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FTP_COLLECT_DURATION_DOC)
                .define(FTP_TIME_OFFSET_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FTP_TIME_OFFSET_DOC)
                .define(FTP_CONNECT_TIMEOUT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FTP_CONNECT_TIMEOUT_DOC)
                .define(FTP_READ_TIMEOUT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FTP_READ_TIMEOUT_DOC);
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

    public String getFtpUrl() { return this.getString(FTP_URL_CONF); }

    public String getFtpPort() { return this.getString(FTP_PORT_CONF); }

    public String getFtpId() { return this.getString(FTP_ID_CONF); }

    public String getFtpPswd() { return this.getString(FTP_PSWD_CONF); }

    public String getFtpDuration() { return this.getString(FTP_COLLECT_DURATION_CONF); }

    public String getFtpTimeOffset() { return this.getString(FTP_TIME_OFFSET_CONF); }

    public String getFtpConnectTimeout() { return this.getString(FTP_CONNECT_TIMEOUT_CONF); }

    public String getFtpReadTimeout() { return this.getString(FTP_READ_TIMEOUT_CONF); }

}
