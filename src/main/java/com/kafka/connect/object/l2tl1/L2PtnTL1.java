package com.kafka.connect.object.l2tl1;

import com.kafka.connect.object.FileObject;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class L2PtnTL1 {

    static final Logger log = LoggerFactory.getLogger(L2PtnTL1.class);

    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Map<String, ?> SOURCE_PARTITION = Collections.emptyMap();
    private static final Map<String, ?> SOURCE_OFFSET = Collections.emptyMap();
    private org.apache.avro.Schema avroSchema;
    private AvroData avroData;
    private String fileName;
    private List<String> contents;
    public List<SourceRecord> records;
    public String header;

    public L2PtnTL1(String fileName, List<String> contents) {
        this.fileName = fileName;
        this.contents = new ArrayList<>();

        header = "";

        for (String content: contents) {
            String tempContent = "";
            String headerTemp[] = content.split("\\/\\*");

            String mainContents = headerTemp[0].replaceAll("(\r\n|\r|\n|\n\r)", " ").trim();
            String subContents = "/* " + headerTemp[1].replaceAll("(\r\n|\r|\n|\n\r)", " ").replaceAll(";", "").trim();

            String[] splitMainContents = mainContents.split("\\s+");

            for (int i = 0; i < splitMainContents.length; i++) {
                switch (i) {
                    case 0:
                        tempContent += splitMainContents[i] + "|";
                        break;
                    case 1:
                        tempContent += splitMainContents[i] + " ";
                        break;
                    case 2:
                        tempContent += splitMainContents[i] + "|";
                        break;
                    case 3:
                        tempContent += splitMainContents[i] + "|";
                        break;
                    case 4:
                        tempContent += splitMainContents[i] + "|";
                        break;
                    default:
                        tempContent += splitMainContents[i] + " ";
                        break;
                }
            }

            tempContent = tempContent.trim() + "|" + subContents;
            log.info(tempContent);

            this.contents.add(tempContent);
        }

        if (this.contents.size() == 0) {
            return;
        }

        int count = countMatches(this.contents.get(0), "|");

        log.info(String.valueOf(count));

        for (int i = 0; i < count; i++) {
            header += (i + 1) + "|";
        }

        header += (count + 1);
        log.info(header);
    }

    public void setMessageSchma(org.apache.avro.Schema avroSchema, AvroData avroData) {
        this.avroSchema = avroSchema;
        this.avroData = avroData;
    }

    public void setMessageValue(String topic) throws IOException, SAXException, ParserConfigurationException, XPathExpressionException {

        String[] splitHeader = header.split("\\|");
        String[] tempHeader = mappingHeader(splitHeader);

        records = new ArrayList<>();

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

            log.info(record.toString());

            records.add(record);
        }
    }

    public List<SourceRecord> getSourceRecord() {
        log.info(records.toString());
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

    private String[] mappingHeader(String headers[]) throws IOException, SAXException, ParserConfigurationException, XPathExpressionException {

        String[] tempList = new String[headers.length];

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("l2ptn.arm.header.mapping.xml");
        Document document = parseXML(inputStream);

        XPath xPath = XPathFactory.newInstance().newXPath();

        for (int i = 0; i < headers.length; i++) {
            String expression = "//*[@id='" + headers[i] + "']";
            String temp = (String) xPath.evaluate(expression, document, XPathConstants.STRING);
            tempList[i] = temp.isEmpty() ? headers[i] : temp;
        }

        return tempList;
    }

    private int countMatches(String text, String rexp) {
        int lineCnt = 0;
        int fromIndex = -1;
        while ((fromIndex = text.indexOf(rexp, fromIndex + 1)) >= 0) {
            lineCnt++;
        }

        return lineCnt;
    }

}