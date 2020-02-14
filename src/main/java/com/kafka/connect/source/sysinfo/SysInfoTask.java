/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.kafka.connect.source.sysinfo;

import com.sun.management.OperatingSystemMXBean;
import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.Collectors;

public class SysInfoTask extends SourceTask {

    static final Logger log = LoggerFactory.getLogger(SysInfoTask.class);

    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Map<String, ?> SOURCE_PARTITION = Collections.emptyMap();
    private static final Map<String, ?> SOURCE_OFFSET = Collections.emptyMap();
    private final OperatingSystemMXBean operatingSystemMXBean
            = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);

    private SysInfoConnectorConfig config;
    private String topic;
    private long maxInterval;
    private int maxRecords;
    private long count = 0L;
    private String schemaFilename;
    private String schemaKeyField;
    private Quickstart quickstart;
    private Generator generator;
    private org.apache.avro.Schema avroSchema;
    private Schema ksqlSchema;
    private AvroData avroData;
    private int idIndex;

    protected enum Quickstart {
        SYSINFO("sysinfo_schema.avro", "id");

        private final String schemaFilename;
        private final String keyName;

        Quickstart(String schemaFilename, String keyName) {
            this.schemaFilename = schemaFilename;
            this.keyName = keyName;
        }

        public String getSchemaFilename() {
            return schemaFilename;
        }

        public String getSchemaKeyField() {
            return keyName;
        }
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new SysInfoConnectorConfig(props);
        topic = config.getKafkaTopic();
        maxInterval = config.getMaxInterval();
        maxRecords = config.getIterations();
        schemaFilename = config.getSchemaFilename();
        schemaKeyField = config.getSchemaKeyfield();

        log.info("jisuk, jisuk");

        String quickstartName = config.getQuickstart();
        log.info("Quick Start : ", Quickstart.valueOf(quickstartName.toUpperCase()));

        if (quickstartName != "") {
            try {
                quickstart = Quickstart.valueOf(quickstartName.toUpperCase());
                idIndex = 0;
                if (quickstart != null) {
                    schemaFilename = quickstart.getSchemaFilename();
                    schemaKeyField = quickstart.getSchemaKeyField();
                    try {
                        generator = new Generator(
                                getClass().getClassLoader().getResourceAsStream(schemaFilename),
                                new Random()
                        );
                    } catch (IOException e) {
                        throw new ConnectException("Unable to read the '"
                                + schemaFilename + "' schema file", e);
                    }
                }
            } catch (IllegalArgumentException e) {
                log.warn("Quickstart '{}' not found: ", quickstartName, e);
            }
        } else {
            try {
                generator = new Generator(
                        new FileInputStream(schemaFilename),
                        new Random()
                );
            } catch (IOException e) {
                throw new ConnectException("Unable to read the '"
                        + schemaFilename + "' schema file", e);
            }
        }

        avroSchema = generator.schema();
        avroData = new AvroData(1);
        ksqlSchema = avroData.toConnectSchema(avroSchema);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        if (maxInterval > 0) {
            try {
                Thread.sleep((long) (maxInterval * Math.random()));
            } catch (InterruptedException e) {
                Thread.interrupted();
                return null;
            }
        }

        final Object generatedObject = generator.generate();
        if (!(generatedObject instanceof GenericRecord)) {
            throw new RuntimeException(String.format(
                    "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
                    generatedObject.getClass().getName()
            ));
        }
        final GenericRecord randomAvroMessage = (GenericRecord) generatedObject;

        final List<Object> genericRowValues = new ArrayList<>();
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            final Object value = randomAvroMessage.get(field.name());
            if (value instanceof Record) {
                final Record record = (Record) value;
                final Object ksqlValue = avroData.toConnectData(record.getSchema(), record).value();
                Object optionValue = getOptionalValue(ksqlSchema.field(field.name()).schema(), ksqlValue);
                genericRowValues.add(optionValue);
            } else {
                genericRowValues.add(value);
            }
        }

        // Key
        String keyString = "";
        if (!schemaKeyField.isEmpty()) {
            SchemaAndValue schemaAndValue = avroData.toConnectData(
                    randomAvroMessage.getSchema().getField(schemaKeyField).schema(),
                    randomAvroMessage.get(schemaKeyField)
            );
            keyString = schemaAndValue.value().toString();
        }

        // Value
        final Schema messageSchema = avroData.toConnectSchema(avroSchema);
        Object messageValue = avroData.toConnectData(avroSchema, randomAvroMessage).value();

        if (maxRecords > 0 && count >= maxRecords) {
            throw new ConnectException(
                    String.format("Stopping connector: generated the configured %d number of messages", count)
            );
        }

        final List<SourceRecord> records = new ArrayList<>();
        SourceRecord record = new SourceRecord(
                SOURCE_PARTITION,
                SOURCE_OFFSET,
                topic,
                KEY_SCHEMA,
                keyString,
                messageSchema,
                messageValue
        );
        records.add(record);
        count += records.size();
        return records;
    }

    @Override
    public void stop() {
    }

    private Schema getOptionalSchema(
            final Schema schema
    ) {
        switch (schema.type()) {
            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case INT32:
                return Schema.OPTIONAL_INT32_SCHEMA;
            case INT64:
                return Schema.OPTIONAL_INT64_SCHEMA;
            case FLOAT64:
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case STRING:
                return Schema.OPTIONAL_STRING_SCHEMA;
            case ARRAY:
                return SchemaBuilder.array(getOptionalSchema(schema.valueSchema())).optional().build();
            case MAP:
                return SchemaBuilder.map(
                        getOptionalSchema(schema.keySchema()),
                        getOptionalSchema(schema.valueSchema())
                ).optional().build();
            case STRUCT:
                final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
                for (Field field : schema.fields()) {
                    schemaBuilder.field(
                            field.name(),
                            getOptionalSchema(field.schema())
                    );
                }
                return schemaBuilder.optional().build();
            default:
                throw new ConnectException("Unsupported type: " + schema);
        }
    }

    private Object getOptionalValue(
            final Schema schema,
            final Object value
    ) {
        switch (schema.type()) {
            case BOOLEAN:
            case INT32:
            case INT64:
            case FLOAT64:
            case STRING:
                return value;
            case ARRAY:
                final List<?> list = (List<?>) value;
                return list.stream()
                        .map(listItem -> getOptionalValue(schema.valueSchema(), listItem))
                        .collect(Collectors.toList());
            case MAP:
                final Map<?, ?> map = (Map<?, ?>) value;
                return map.entrySet().stream()
                        .collect(Collectors.toMap(
                                k -> getOptionalValue(schema.keySchema(), k),
                                v -> getOptionalValue(schema.valueSchema(), v)
                        ));
            case STRUCT:
                final Struct struct = (Struct) value;
                final Struct optionalStruct = new Struct(getOptionalSchema(schema));
                for (Field field : schema.fields()) {
                    optionalStruct.put(
                            field.name(),
                            getOptionalValue(
                                    field.schema(),
                                    struct.get(field.name())
                            )
                    );
                }
                return optionalStruct;
            default:
                throw new ConnectException("Invalid value schema: " + schema + ", value = " + value);
        }
    }
}
