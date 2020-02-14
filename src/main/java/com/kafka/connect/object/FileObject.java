package com.kafka.connect.object;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.List;

public abstract class FileObject {
    public String fileName;
    public String header;
    public List<String> contents;
    public org.apache.avro.Schema avroSchema;
    public AvroData avroData;
    public List<SourceRecord> records;

    public FileObject(String fileName, String header, List<String> contents) {
        this.fileName = fileName;
        this.header = header;
        this.contents = contents;
    }

    public FileObject(String fileName, String header, List<String> contents, org.apache.avro.Schema avroSchema, AvroData avroData) {
        this.fileName = fileName;
        this.header = header;
        this.contents = contents;
        this.avroSchema = avroSchema;
        this.avroData = avroData;
    }

    public abstract void setMessageSchma(org.apache.avro.Schema avroSchema, AvroData avroData);
    public abstract void setMessageValue(String topic) throws IOException, SAXException, ParserConfigurationException, XPathExpressionException;

    public abstract List<SourceRecord> getSourceRecord();
}
