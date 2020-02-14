package com.kafka.connect.source.l2ptn.tl1;

import com.kafka.connect.object.l2tl1.L2PtnTL1;
import com.kafka.connect.source.l2ptn.ftp.pm.L2ParserConnectorConfig;
import com.kafka.connect.source.l2ptn.ftp.pm.VersionUtil;
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

public class L2TL1Task extends SourceTask {

    static final Logger log = LoggerFactory.getLogger(L2TL1Task.class);

    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Map<String, ?> SOURCE_PARTITION = Collections.emptyMap();
    private static final Map<String, ?> SOURCE_OFFSET = Collections.emptyMap();

    private L2ParserConnectorConfig config;
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
        L2PTNARM("l2ptn.arm.avro", "_timestamp");

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

        config = new L2ParserConnectorConfig(props);
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
            log.info("---------------------------------------");

            File file = new File(fileName);
            final List<SourceRecord> records = new ArrayList<>();

            try {
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                String line;
                String content = "";
                List<String> contents = new ArrayList<String>();

                while((line = bufferedReader.readLine()) != null) {
                    content += (line + "\n\r");

                    if (line.trim().equals(";")) {
                        content = content.replace(content.substring(content.length() - 1), "");
                        contents.add(content);
                        content = "";
                    }
                }

                setAvroSchemaConfig("L2PTNARM");
                L2PtnTL1 l2PtnTL1 = new L2PtnTL1(fileName, contents);
                l2PtnTL1.setMessageSchma(avroSchema, avroData);

                log.info("finish1");
                l2PtnTL1.setMessageValue(topic);

                log.info("finish2");
                dataSourceList.addAll(l2PtnTL1.getSourceRecord());

                log.info(l2PtnTL1.getSourceRecord().toString());

                testIndex++;

                bufferedReader.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (XPathExpressionException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            }
        }

        log.info("finish");

        return dataSourceList;
    }

    @Override
    public void stop() {
        log.info("Stop");
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
