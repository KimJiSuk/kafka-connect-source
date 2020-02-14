package com.kafka.connect.source.l2ptn.tl1;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class L2TL1ConnectorConfig extends AbstractConfig {
    public static final String TOPICS_CONF = "topics";
    private static final String TOPICS_DOC = "Topic to write to";
    public static final String TOPICS_WORK_CONF = "topics.work";
    private static final String TOPICS_WORK_DOC = "On/Off topic";
//    public static final String FILE_NAME_PREFIX_CONF = "file.name.prefix";
//    private static final String FILE_NAME_PREFIX_DOC = "FileName prefix List";
//    public static final String FILE_NAME_DATEFORMAT_CONF = "file.name.dateformat";
//    private static final String FILE_NAME_DATEFORMAT_DOC = "FileName dateformat";
//    public static final String FILE_NAME_TEMPLATE_CONF = "file.name.template";
//    private static final String FILE_NAME_TEMPLATE_DOC = "FileName Template";
//    public static final String FILE_PATH_CONF = "file.path";
//    private static final String FILE_PATH_DOC = "File Path";
    
    public static final String TCP_IP_CONF = "tcp.ip";
	private static final String TCP_IP_DOC = "TCP IP";	
	public static final String TCP_PORT_CONF = "tcp.port";
	private static final String TCP_PORT_DOC = "TCP PORT";	
	public static final String TCP_BUFFER_SIZE_CONF = "tcp.buffer.size"; // read buffer size
	private static final String TCP_BUFFER_SIZE_DOC = "TCP READ BUFFER SIZE";
	public static final String TCP_CHARSET_CONF = "tcp.charset";
	private static final String TCP_CHARSET_DOC = "TCP Read Charset";
	public static final String POLL_SLEEP_TIME_CONF = "poll.sleep.time";
	private static final String POLL_SLEEP_TIME_DOC = "Poll Sleep time";
//	public static final String TCP_RECONN_WAIT_TIME_CONF = "tcp.reconn.wait.time";
//	private static final String TCP_RECONN_WAIT_TIME_DOC = "TCP RECONNECT WAIT TIME";
	
    public L2TL1ConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
        validate();
    }

    public L2TL1ConnectorConfig(Map<String, String> parsedConfig) {
        this(configDef(), parsedConfig);
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(TOPICS_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, TOPICS_DOC)
                .define(TOPICS_WORK_CONF, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, TOPICS_WORK_DOC)
                
                .define(TCP_IP_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, TCP_IP_DOC)
				.define(TCP_PORT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, TCP_PORT_DOC)
				.define(TCP_BUFFER_SIZE_CONF, ConfigDef.Type.STRING, "4096", ConfigDef.Importance.HIGH, TCP_BUFFER_SIZE_DOC)
				.define(TCP_CHARSET_CONF, ConfigDef.Type.STRING, "UTF-8", ConfigDef.Importance.HIGH, TCP_CHARSET_DOC)
				.define(POLL_SLEEP_TIME_CONF, ConfigDef.Type.STRING, "UTF-8", ConfigDef.Importance.HIGH, POLL_SLEEP_TIME_DOC);
        
//				.define(TCP_RECONN_WAIT_TIME_CONF, ConfigDef.Type.STRING, "1000", ConfigDef.Importance.HIGH, TCP_RECONN_WAIT_TIME_DOC);
                
//                .define(FILE_NAME_PREFIX_CONF, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH, FILE_NAME_PREFIX_DOC)
//                .define(FILE_NAME_DATEFORMAT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_NAME_DATEFORMAT_DOC)
//                .define(FILE_NAME_TEMPLATE_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_NAME_TEMPLATE_DOC)
//                .define(FILE_PATH_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, FILE_PATH_DOC);
    }

    private void validate() {}

    public List getTopics() { return this.getList(TOPICS_CONF); }

    public List getTopicsWork() { return this.getList(TOPICS_WORK_CONF); }

//    public List getFileNamePrefix() { return this.getList(FILE_NAME_PREFIX_CONF); }
//
//    public String getFileNameDateformat() { return this.getString(FILE_NAME_DATEFORMAT_CONF); }
//
//    public String getFileNameTemplate() { return this.getString(FILE_NAME_TEMPLATE_CONF); }
//
//    public String getFilePath() { return this.getString(FILE_PATH_CONF); }
        
    public String getTcpIp() { return this.getString(TCP_IP_CONF); }
	
	public String getTcpPort() { return this.getString(TCP_PORT_CONF); }
	
	public String getTcpBufferSize() { return this.getString(TCP_BUFFER_SIZE_CONF); }
	
	public String getTcpCharset() { return this.getString(TCP_CHARSET_CONF); }
	
	public String getPollSleepTime() { return this.getString(POLL_SLEEP_TIME_CONF); }
	
//	public String getTcpReconnWaitTime() { return this.getString(TCP_RECONN_WAIT_TIME_CONF); }
}
