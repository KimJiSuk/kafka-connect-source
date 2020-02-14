package com.kafka.connect.object.l2file;

import com.kafka.connect.object.FileObject;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class L2PtnTunnel extends FileObject {

    static final Logger log = LoggerFactory.getLogger(L2PtnTunnel.class);

    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Map<String, ?> SOURCE_PARTITION = Collections.emptyMap();
    private static final Map<String, ?> SOURCE_OFFSET = Collections.emptyMap();
    private org.apache.avro.Schema avroSchema;
    private AvroData avroData;

    public L2PtnTunnel(String fileName, String header, List<String> contents) {
        super(fileName, header, contents);
        records = new ArrayList<SourceRecord>();
    }


    @Override
    public void setMessageSchma(org.apache.avro.Schema avroSchema, AvroData avroData) {
        this.avroSchema = avroSchema;
        this.avroData = avroData;
    }

    @Override
    public void setMessageValue(String topic) throws IOException, SAXException, ParserConfigurationException, XPathExpressionException {

        header = header.replaceAll("\\/\\*", "").replaceAll("\\*\\/", "").trim();

        String[] splitHeader = header.split("\\|");
        String[] tempHeader = mappingHeader(splitHeader);

        for (String content: contents) {
            String[] tempContent = content.trim().split("\\|");

            if(tempHeader.length != tempContent.length) {
                continue;
            }

            GenericRecord genericRecord = generateRecord(avroSchema, tempHeader, tempContent);

            final Schema messageSchema = avroData.toConnectSchema(avroSchema);
            final Object messageValue = avroData.toConnectData(avroSchema, genericRecord).value();

            SourceRecord record = new SourceRecord(
                    SOURCE_PARTITION,
                    SOURCE_OFFSET,
                    topic,
                    KEY_SCHEMA,
                    "",
                    messageSchema,
                    messageValue
            );

            records.add(record);
        }
    }

    @Override
    public List<SourceRecord> getSourceRecord() {
        return records;
    }

    private GenericRecord generateRecord(org.apache.avro.Schema schema, String headers[], String values[]) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            for (int i = 0; i < headers.length; i++) {
                if(field.name().equals(headers[i])) {
                    Object obj = generateObject(field.schema(), values[i]);
                    builder.set(field, obj);
                    break;
                }
            }
        }

        return builder.build();
    }
    private Object generateObject(org.apache.avro.Schema schema, String value) {
        switch (schema.getType()) {
            case BOOLEAN:
                return Boolean.valueOf(value);
            case BYTES:
                return Byte.valueOf(value);
            case DOUBLE:
                return Double.valueOf(value);
            case FLOAT:
                return Float.valueOf(value);
            case INT:
                return Integer.valueOf(value);
            case LONG:
                return Long.valueOf(value);
            case NULL:
                return null;
            case STRING:
                return value;
            default:
                throw new RuntimeException("Unrecognized schema type: " + schema.getType());
        }
    }

    private Document parseXML(InputStream stream) throws ParserConfigurationException, IOException, SAXException {

        DocumentBuilderFactory objDocumentBuilderFactory = null;
        DocumentBuilder objDocumentBuilder = null;
        Document doc = null;

        objDocumentBuilderFactory = DocumentBuilderFactory.newInstance();
        objDocumentBuilder = objDocumentBuilderFactory.newDocumentBuilder();

        doc = objDocumentBuilder.parse(stream);

        return doc;
    }

    private String[] mappingHeader(String[] headers) throws IOException, SAXException, ParserConfigurationException, XPathExpressionException {

        String[] tempList = new String[headers.length];

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("l2ptn.tunnel.header.mapping.xml");
        Document document = parseXML(inputStream);

        XPath xPath = XPathFactory.newInstance().newXPath();

        for (int i = 0; i < headers.length; i++) {
            String expression = "//*[@id='" + headers[i] + "']";
            String temp = (String) xPath.evaluate(expression, document, XPathConstants.STRING);
            tempList[i] = temp.isEmpty() ? headers[i] : temp;
        }

        return tempList;
    }

}