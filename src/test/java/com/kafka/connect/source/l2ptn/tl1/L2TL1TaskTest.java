package com.kafka.connect.source.l2ptn.tl1;

import com.kafka.connect.source.l2ptn.tl1.L2TL1ConnectorConfig;
import com.kafka.connect.source.l2ptn.tl1.L2TL1Task;
import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class L2TL1TaskTest {

    static final Logger log = LoggerFactory.getLogger(L2TL1TaskTest.class);

    private static final String TOPICS = "l2ptnarm";
    private static final String TOPICS_WORK = "1,1,0,0,1";
    private static final String FILE_NAME_PREFIX = "PTN_ARM";
    private static final String FILE_NAME_DATEFORMAT = "yyyyMMdd_HHmm";
    private static final String FILE_NAME_TEMPLATE = "{{prefix}}_{{dateformat}}.txt";
    private static final String FILE_PATH = "/Users/js/Documents/mobigen/gitlab/kafka-connect/FTP_Sample/";

    private static final String TCP_IP_CONF = "39.117.20.222";
    private static final String TCP_PORT_CONF = "19012";
    private static final String TCP_BUFFER_SIZE_CONF = "4096";
    private static final String TCP_CHARSET_CONF = "MS949";
    private static final String POLL_SLEEP_TIME_CONF = "500";

    private static final int NUM_MESSAGES = 100;
    private static final int MAX_INTERVAL_MS = 0;

    private static final AvroData AVRO_DATA = new AvroData(20);

    private Map<String, String> config;
    private L2TL1Task task;
    private List<SourceRecord> records;
    private Schema expectedValueConnectSchema;
    private Schema expectedKeyConnectSchema;

    @Before
    public void setUp() throws Exception {
        config = new HashMap<>();
        records = new ArrayList<>();
    }

    @After
    public void tearDown() throws Exception {
//        task.stop();
//        task = null;
    }

    @Test
    public void shouldFailToGenerateMoreRecordsThanSpecified() throws Exception {
        // Generate the expected number of records
//        createTaskWith(L2TL1Task.AvroFile.L2PTNARM);
//        generateRecords();
        //assertRecordsMatchSchemas();

        // Attempt to get another batch of records, but the task is expected to fail
        try {
//            task.poll();
        } catch (ConnectException e) {
            // expected
        }
    }

    private void generateAndValidateRecordsFor(L2TL1Task.AvroFile avroFile) throws Exception {
        createTaskWith(avroFile);
        generateRecords();
        //assertRecordsMatchSchemas();

        // Do the same thing with schema file
        createTaskWithSchema(avroFile.getSchemaFilename(), avroFile.getSchemaKeyField());
        generateRecords();
        //assertRecordsMatchSchemas();
    }

    private void generateRecords() throws Exception {
        records.clear();
//        while (records.size() < NUM_MESSAGES) {
//            List<SourceRecord> newRecords = task.poll();
//            assertNotNull(newRecords);
//            records.addAll(newRecords);
//
//            log.info(String.valueOf(records.size()));
//        }
    }

    private void assertRecordsMatchSchemas() {
        for (SourceRecord record : records) {
            // Check the key
            assertEquals(expectedKeyConnectSchema, record.keySchema());
            if (expectedKeyConnectSchema != null) {
                assertTrue(isConnectInstance(record.key(), expectedKeyConnectSchema));
            }

            // Check the value
            assertEquals(expectedValueConnectSchema, record.valueSchema());
            if (expectedValueConnectSchema != null) {
                assertTrue(isConnectInstance(record.value(), expectedValueConnectSchema));
            }
        }
    }

    private boolean isConnectInstance(Object value, Schema expected) {
        if (expected.isOptional() && value == null) {
            return true;
        }
        switch (expected.type()) {
            case BOOLEAN:
                return value instanceof Boolean;
            case BYTES:
                return value instanceof byte[];
            case INT8:
                return value instanceof Byte;
            case INT16:
                return value instanceof Short;
            case INT32:
                return value instanceof Integer;
            case INT64:
                return value instanceof Long;
            case FLOAT32:
                return value instanceof Float;
            case FLOAT64:
                return value instanceof Double;
            case STRING:
                return value instanceof String;
            case ARRAY:
                return value instanceof List;
            case MAP:
                return value instanceof Map;
            case STRUCT:
                if (value instanceof Struct) {
                    Struct struct = (Struct) value;
                    for (Field field : expected.fields()) {
                        Object fieldValue = struct.get(field.name());
                        if (!isConnectInstance(fieldValue, field.schema())) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            default:
                throw new IllegalArgumentException("Unexpected enum schema");
        }
    }

    private void createTaskWith(L2TL1Task.AvroFile avroFile) {
        createTask();
        loadKeyAndValueSchemas(avroFile.getSchemaFilename(), avroFile.getSchemaKeyField());
    }

    private void createTaskWithSchema(String schemaResourceFilename, String idFieldName) {
        createTask();
        loadKeyAndValueSchemas(schemaResourceFilename, idFieldName);
    }

    private void createTask() {
        config.putIfAbsent(L2TL1ConnectorConfig.TOPICS_CONF, TOPICS);
        config.putIfAbsent(L2TL1ConnectorConfig.TOPICS_WORK_CONF, TOPICS_WORK);
        config.putIfAbsent(L2TL1ConnectorConfig.TCP_IP_CONF, TCP_IP_CONF);
        config.putIfAbsent(L2TL1ConnectorConfig.TCP_PORT_CONF, TCP_PORT_CONF);
        config.putIfAbsent(L2TL1ConnectorConfig.TCP_BUFFER_SIZE_CONF, TCP_BUFFER_SIZE_CONF);
        config.putIfAbsent(L2TL1ConnectorConfig.TCP_CHARSET_CONF, TCP_CHARSET_CONF);
        config.putIfAbsent(L2TL1ConnectorConfig.POLL_SLEEP_TIME_CONF, POLL_SLEEP_TIME_CONF);

//        task = new L2TL1Task();
//        task.start(config);
    }

    private void loadKeyAndValueSchemas(String schemaResourceFilename, String idFieldName) {
        org.apache.avro.Schema expectedValueAvroSchema = loadAvroSchema(schemaResourceFilename);
        expectedValueConnectSchema = AVRO_DATA.toConnectSchema(expectedValueAvroSchema);

        if (idFieldName != null) {
            // Check that the Avro schema has the named field
            org.apache.avro.Schema expectedKeyAvroSchema = expectedValueAvroSchema.getField(idFieldName).schema();
            assertNotNull(expectedKeyAvroSchema);
            expectedKeyConnectSchema = AVRO_DATA.toConnectSchema(expectedKeyAvroSchema);
        }

        // Right now, Datagen always uses non-null strings for the key!
        expectedKeyConnectSchema = Schema.STRING_SCHEMA;
    }

    private org.apache.avro.Schema loadAvroSchema(String schemaFilename) {
        try {
            // log.info("-------------------------- loadAvroSchema --------------------------");
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(schemaFilename);
            // log.info(inputStream.toString());
            org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(inputStream);
            // log.info(schema.toString());
            return schema;
        } catch (IOException e) {
            throw new ConnectException("Unable to read the '" + schemaFilename + "' schema file", e);
        }
    }
}
