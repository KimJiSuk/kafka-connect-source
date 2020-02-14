package com.kafka.connect.source.l2ptn.ftp.pm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class L2ParserConnectorTest {
    private static final String TOPICS = "l2ptntunnel,l2ptnac,l2ptnpm,l2ptnport,l2ptnpw";
    private static final String TOPICS_WORK = "1,1,0,0,1";
    private static final String FILE_NAME_PREFIX = "PM_TUNNEL,PM_AC,PM,PM_PORT,PM_PW";
    private static final String FILE_NAME_DATEFORMAT = "yyyyMMdd_HH:mm";
    private static final String FILE_NAME_TEMPLATE = "{{prefix}}_{{dateformat}}.txt";
    private static final String FILE_PATH = "/Users/js/Documents/mobigen/gitlab/kafka-connect/FTP_Sample/";

    private Map<String, String> config;
    private L2ParserConnector connector;

    @Before
    public void setUp() throws Exception {
        config = new HashMap<>();
        config.put(L2ParserConnectorConfig.TOPICS_CONF, TOPICS);
        config.put(L2ParserConnectorConfig.TOPICS_WORK_CONF, TOPICS_WORK);
        config.put(L2ParserConnectorConfig.FILE_NAME_PREFIX_CONF, FILE_NAME_PREFIX);
        config.put(L2ParserConnectorConfig.FILE_NAME_DATEFORMAT_CONF, FILE_NAME_DATEFORMAT);
        config.put(L2ParserConnectorConfig.FILE_NAME_TEMPLATE_CONF, FILE_NAME_TEMPLATE);
        config.put(L2ParserConnectorConfig.FILE_PATH_CONF, FILE_PATH);
        connector = new L2ParserConnector();
    }

    @After
    public void tearDown() throws Exception {
        connector.stop();
    }

    @Test
    public void shouldCreateTasks() {
        connector.start(config);

        assertTaskConfigs(1);
        assertTaskConfigs(2);
        assertTaskConfigs(4);
        assertTaskConfigs(10);
        for (int i=0; i!=100; ++i) {
            assertTaskConfigs(0);
        }
    }

    protected void assertTaskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        assertEquals(maxTasks, taskConfigs.size());
        // All task configs should match the connector config
        for (Map<String, String> taskConfig : taskConfigs) {
            assertEquals(config, taskConfig);
        }
    }
}
