package com.kafka.connect.source.l2ptn.ftp.pm;

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
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

public class L2ParserTaskTest {

    static final Logger log = LoggerFactory.getLogger(L2ParserTaskTest.class);

    private static final String TOPICS = "l2ptntunnel,l2ptnac,l2ptnpm,l2ptnport,l2ptnpw";
    private static final String TOPICS_WORK = "1,1,0,0,1";
    private static final String FILE_NAME_PREFIX = "PM_TUNNEL,PM_AC,PM,PM_PORT,PM_PW";
    private static final String FILE_NAME_DATEFORMAT = "yyyyMMdd_HHmm";
    private static final String FILE_NAME_TEMPLATE = "{{prefix}}_{{dateformat}}.txt";
    private static final String FILE_PATH = "/Users/js/Documents/mobigen/gitlab/kafka-connect/FTP_Sample/";
    private static final int NUM_MESSAGES = 100;
    private static final int MAX_INTERVAL_MS = 0;

    private static final AvroData AVRO_DATA = new AvroData(20);

    private Map<String, String> config;
    private L2ParserTask task;
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
//        createTaskWith(L2ParserTask.AvroFile.L2PTNTUNNEL);
//        generateRecords();
//        //assertRecordsMatchSchemas();
//
//        // Attempt to get another batch of records, but the task is expected to fail
//        try {
//            task.poll();
//        } catch (ConnectException e) {
//            // expected
//        }
    }

    private void generateAndValidateRecordsFor(L2ParserTask.AvroFile avroFile) throws Exception {
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
        while (records.size() < NUM_MESSAGES) {
            List<SourceRecord> newRecords = task.poll();
            assertNotNull(newRecords);
            records.addAll(newRecords);

            log.info(String.valueOf(records.size()));
        }
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

    private void createTaskWith(L2ParserTask.AvroFile avroFile) {
        createTask();
        loadKeyAndValueSchemas(avroFile.getSchemaFilename(), avroFile.getSchemaKeyField());
    }

    private void createTaskWithSchema(String schemaResourceFilename, String idFieldName) {
        createTask();
        loadKeyAndValueSchemas(schemaResourceFilename, idFieldName);
    }

    private void createTask() {
        config.putIfAbsent(L2ParserConnectorConfig.TOPICS_CONF, TOPICS);
        config.putIfAbsent(L2ParserConnectorConfig.TOPICS_WORK_CONF, TOPICS_WORK);
        config.putIfAbsent(L2ParserConnectorConfig.FILE_NAME_PREFIX_CONF, FILE_NAME_PREFIX);
        config.putIfAbsent(L2ParserConnectorConfig.FILE_NAME_TEMPLATE_CONF, FILE_NAME_TEMPLATE);
        config.putIfAbsent(L2ParserConnectorConfig.FILE_NAME_DATEFORMAT_CONF, FILE_NAME_DATEFORMAT);
        config.putIfAbsent(L2ParserConnectorConfig.FILE_PATH_CONF, FILE_PATH);

        task = new L2ParserTask();
        task.start(config);
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
