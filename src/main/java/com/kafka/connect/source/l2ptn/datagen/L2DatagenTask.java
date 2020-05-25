package com.kafka.connect.source.l2ptn.datagen;

import com.kafka.connect.object.l2file.*;
import com.kafka.connect.templating.Template;
import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class L2DatagenTask extends SourceTask {

    static final Logger log = LoggerFactory.getLogger(L2DatagenTask.class);

    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Map<String, ?> SOURCE_PARTITION = Collections.emptyMap();
    private static final Map<String, ?> SOURCE_OFFSET = Collections.emptyMap();

    private L2DatagenConnectorConfig config;
    private List topics;
    private List topicsWork;
    private List fileNamePrefix;
    private String filePath;
    private String fileNameDateformat;
    private String fileNameTemplete;

    private AvroFile avroFile;
    private org.apache.avro.Schema avroSchema;
    private AvroData avroData;
    private int testIndex = 0;
    private Calendar calendar = Calendar.getInstance();

    protected enum AvroFile {
        L2PTNTUNNEL("l2ptn.tunnel.avro", "_timestamp"),
        L2PTNAC("l2ptn.ac.avro", "_timestamp"),
        L2PTNPM("l2ptn.pm.avro", "_timestamp"),
        L2PTNPORT("l2ptn.port.avro", "_timestamp"),
        L2PTNPW("l2ptn.pw.avro", "_timestamp");

        private final String schemaFilename;
        private final String keyName;

        AvroFile(String schemaFilename, String keyName) {
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
        Objects.requireNonNull(props);

        config = new L2DatagenConnectorConfig(props);
        topics = config.getTopics();
        topicsWork = config.getTopicsWork();
        fileNamePrefix = config.getFileNamePrefix();
        fileNameDateformat = config.getFileNameDateformat();
        fileNameTemplete = config.getFileNameTemplate();
        filePath = config.getFilePath();

        calendar.set(Calendar.YEAR, 2019);
        calendar.set(Calendar.MONTH, 8);
        calendar.set(Calendar.DATE, 27);
        calendar.set(Calendar.HOUR_OF_DAY, 11);
        calendar.set(Calendar.MINUTE, 15);
    }

    public void setAvroSchemaConfig(String avroName) {

        try {
            log.info(avroName);
            avroFile = AvroFile.valueOf(avroName.toUpperCase());
            if (avroFile != null) {
                try {
                    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(avroFile.getSchemaFilename());
                    avroSchema = new org.apache.avro.Schema.Parser().parse(inputStream);
                } catch (IOException e) {
                    throw new ConnectException("Unable to read the '"
                            + avroFile.getSchemaFilename() + "' schema file", e);
                }
            }
        } catch (IllegalArgumentException e) {
            log.warn("AvroFile '{}' not found: ", avroName, e);
        }

        avroData = new AvroData(1);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        try {
            Thread.sleep((long) (1000 * Math.random()));
        } catch (InterruptedException e) {
            Thread.interrupted();
            return null;
        }

        log.info("testIndex : " + String.valueOf(testIndex));

        List<SourceRecord> dataSourceList = new ArrayList<>();

        for (int i = 0; i < topics.size(); i++) {
            String topic = String.valueOf(topics.get(i));
            String prefix = String.valueOf(fileNamePrefix.get(i));

            String fileName = filePath + getFileName(prefix);

            log.info("---------------------------------------");
            log.info("FileName : " + fileName);
            log.info("Topics.size : " + String.valueOf(topics.size()));
            log.info("Topics.i : " + String.valueOf(i));
            log.info("---------------------------------------");

            File file = new File(fileName);
            final List<SourceRecord> records = new ArrayList<>();

            try {
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                boolean headerFlag = true;

                String line;
                String header = "";
                List<String> contents = new ArrayList<String>();

                while((line = bufferedReader.readLine()) != null) {
                    if(headerFlag) {
                        header = line;
                        headerFlag = false;
                        continue;
                    }

                    contents.add(line);
                }

                if(topic.contains("tunnel")) {
                    setAvroSchemaConfig("L2PTNTUNNEL");
                    L2PtnTunnel l2PtnTunnel = new L2PtnTunnel(fileName, header, contents);
                    l2PtnTunnel.setMessageSchma(avroSchema, avroData);
                    l2PtnTunnel.setMessageValue(topic);
                    dataSourceList.addAll(l2PtnTunnel.getSourceRecord());
                } else if(topic.contains("ac")) {
                    setAvroSchemaConfig("L2PTNAC");
                    L2PtnAc l2PtnAc = new L2PtnAc(fileName, header, contents);
                    l2PtnAc.setMessageSchma(avroSchema, avroData);
                    l2PtnAc.setMessageValue(topic);
                    dataSourceList.addAll(l2PtnAc.getSourceRecord());
                } else if(topic.contains("pm")) {
                    setAvroSchemaConfig("L2PTNPM");
                    L2PtnPm l2PtnPm = new L2PtnPm(fileName, header, contents);
                    l2PtnPm.setMessageSchma(avroSchema, avroData);
                    l2PtnPm.setMessageValue(topic);
                    dataSourceList.addAll(l2PtnPm.getSourceRecord());
                } else if(topic.contains("port")) {
                    setAvroSchemaConfig("L2PTNPORT");
                    L2PtnPort l2PtnPort = new L2PtnPort(fileName, header, contents);
                    l2PtnPort.setMessageSchma(avroSchema, avroData);
                    l2PtnPort.setMessageValue(topic);
                    dataSourceList.addAll(l2PtnPort.getSourceRecord());
                } else if(topic.contains("pw")){
                    setAvroSchemaConfig("L2PTNPW");
                    L2PtnPw l2PtnPw = new L2PtnPw(fileName, header, contents);
                    l2PtnPw.setMessageSchma(avroSchema, avroData);
                    l2PtnPw.setMessageValue(topic);
                    dataSourceList.addAll(l2PtnPw.getSourceRecord());
                } else if(topic.contains("optic")){
                    setAvroSchemaConfig("L2PTNOPTIC");
                    L2PtnOptic l2PtnOptic = new L2PtnOptic(fileName, header, contents);
                    l2PtnOptic.setMessageSchma(avroSchema, avroData);
                    l2PtnOptic.setMessageValue(topic);
                    dataSourceList.addAll(l2PtnOptic.getSourceRecord());
                } else if(topic.contains("temperature")){
                    setAvroSchemaConfig("L2PTNTEMPERATURE");
                    L2PtnTemperature l2PtnTemperature = new L2PtnTemperature(fileName, header, contents);
                    l2PtnTemperature.setMessageSchma(avroSchema, avroData);
                    l2PtnTemperature.setMessageValue(topic);
                    dataSourceList.addAll(l2PtnTemperature.getSourceRecord());
                } else {
                    return null;
                }

                testIndex++;

                bufferedReader.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (XPathExpressionException e) {
                e.printStackTrace();
            }
        }

        return dataSourceList;
    }

    @Override
    public void stop() {

    }

    private String getFileName(String prefix) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(fileNameDateformat);
        String timeString = dateFormat.format(calendar.getTime());

        Template template = new Template(fileNameTemplete);

        return template.instance()
                .bindVariable("prefix", () -> prefix)
                .bindVariable("dateformat", () -> timeString)
                .render();
    }

}
