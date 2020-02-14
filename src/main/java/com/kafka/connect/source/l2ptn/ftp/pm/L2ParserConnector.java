package com.kafka.connect.source.l2ptn.ftp.pm;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class L2ParserConnector extends SourceConnector {

    private static Logger log = LoggerFactory.getLogger(L2ParserConnector.class);
    private L2ParserConnectorConfig config;
    private Map<String, String> props;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            this.props = props;
            config = new L2ParserConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConfigException("L2 Parser connector could not start because of an error in the configuration: ", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return L2ParserTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(this.props);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return L2ParserConnectorConfig.configDef();
    }
}
