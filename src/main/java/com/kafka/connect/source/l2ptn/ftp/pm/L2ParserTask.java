package com.kafka.connect.source.l2ptn.ftp.pm;

/****************************************************
 * 성능 ftp 파일 처리
 * @author Jun
 ****************************************************/
import com.kafka.connect.object.l2file.*;
import com.kafka.connect.templating.Template;
import io.confluent.connect.avro.AvroData;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
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
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class L2ParserTask extends SourceTask {

	static final Logger log = LoggerFactory.getLogger(L2ParserTask.class);

	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	private static final Map<String, ?> SOURCE_PARTITION = Collections.emptyMap();
	private static final Map<String, ?> SOURCE_OFFSET = Collections.emptyMap();

	private L2ParserConnectorConfig config;
	private List topics;
	// private List topicsWork;
	private List fileNamePrefix;
	// private String filePath;
	private String fileNameDateformat;
//	private String fileNameTemplete;

	private AvroFile avroFile;
	private org.apache.avro.Schema avroSchema;
	private AvroData avroData;
//	private int testIndex = 0;
//	private Calendar calendar = Calendar.getInstance();

	private SimpleDateFormat DATE_FORMAT;
	private String FTP_URL, FTP_ID, FTP_PSWD, FILE_CHARSET;;
	private int FTP_PORT, FTP_COLLECT_DURATION, FTP_CONNECT_TIMEOUT, FTP_READ_TIMEOUT;
	private float FTP_TIME_OFFSET;
	private String COLLECT_SUCCESS_FILE_NAME_PREFIX = "OK_";

	protected enum AvroFile {
		// @formatter:off
        L2PTNTUNNEL("l2ptn.tunnel.avro", "_timestamp"),
        L2PTNAC("l2ptn.ac.avro", "_timestamp"),
        L2PTNPM("l2ptn.pm.avro", "_timestamp"),
        L2PTNPORT("l2ptn.port.avro", "_timestamp"),
        L2PTNPW("l2ptn.pw.avro", "_timestamp"),
		L2PTNOPTIC("l2ptn.optic.avro", "_timestamp"),
		L2PTNTEMPERATURE("l2ptn.temperature.avro", "_timestamp");
        // @formatter:on

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

	public void setAvroSchemaConfig(String avroName) {
		try {
			log.info(avroName);
			avroFile = AvroFile.valueOf(avroName.toUpperCase());
			if (avroFile != null) {
				try {
					InputStream inputStream = getClass().getClassLoader().getResourceAsStream(avroFile.getSchemaFilename());
					avroSchema = new org.apache.avro.Schema.Parser().parse(inputStream);
				} catch (IOException e) {
					throw new ConnectException("Unable to read the '" + avroFile.getSchemaFilename() + "' schema file", e);
				}
			}
		} catch (IllegalArgumentException e) {
			log.warn("AvroFile '{}' not found: ", avroName, e);
		}
		avroData = new AvroData(1);
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
//		topicsWork = config.getTopicsWork();
		fileNamePrefix = config.getFileNamePrefix();
		fileNameDateformat = config.getFileNameDateformat();
		FILE_CHARSET = config.getFileCharset();

		FTP_URL = config.getFtpUrl();
		FTP_PORT = Integer.parseInt(config.getFtpPort());
		FTP_ID = config.getFtpId();
		FTP_PSWD = config.getFtpPswd();
		FTP_COLLECT_DURATION = Integer.parseInt(config.getFtpDuration());
		FTP_TIME_OFFSET = Float.parseFloat(config.getFtpTimeOffset());
		FTP_CONNECT_TIMEOUT = Integer.parseInt(config.getFtpConnectTimeout());
		FTP_READ_TIMEOUT = Integer.parseInt(config.getFtpReadTimeout());
		DATE_FORMAT = new SimpleDateFormat(fileNameDateformat);

		TimeZone tz = Calendar.getInstance().getTimeZone();
		log.info("### TIME ZONE INFO : " + tz.getDisplayName() + ", " + tz.getID());
	}

	@Override
	public void stop() {
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {

		// 수집 대기
		try {
			Thread.sleep(FTP_COLLECT_DURATION);
		} catch (Exception e) {
			Thread.interrupted();
			return null;
		}
		log.info("########################################################################################################");

		// Ftp 연결 시작
		List<SourceRecord> resultDataSourceList = null;
		FTPClient ftpClient = null;

		try { // FTP 연결 후 로그인
			ftpClient = new FTPClient();
			ftpClient.setConnectTimeout(FTP_CONNECT_TIMEOUT); // 연결 시간 설정
			ftpClient.connect(FTP_URL, FTP_PORT);
			ftpClient.setSoTimeout(FTP_READ_TIMEOUT); // read & write timeout 설정

			// FTP 연결 실패
			if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode()) == false) {
				log.info("FTP CONNECT ERROR, REPLY CODE : " + ftpClient.getReplyCode());
			} else {
				ftpClient.setFileTransferMode(FTPClient.BINARY_FILE_TYPE);

				// FTP 로그인 성공
				if (ftpClient.login(FTP_ID, FTP_PSWD) == true) {

					ftpClient.setFileTransferMode(FTPClient.BINARY_FILE_TYPE);
					ftpClient.enterLocalPassiveMode(); // passive mode 로 연결해야 함

					// 모든 대상 FTP 파일 수집 후 결과 합치기
					String collectTimeStr = getCollectTime();

					for (int i = 0; i < fileNamePrefix.size(); i++) {
						List<SourceRecord> dataSourceList = collectFtpFile(ftpClient, collectTimeStr, i);
						if (dataSourceList != null) {
							if (resultDataSourceList == null) {
								resultDataSourceList = dataSourceList;
							} else {
								for (SourceRecord data : dataSourceList)
									resultDataSourceList.add(data);
							}
						}
					}
				} else { // FTP 로그인 실패
					log.info("FTP LOGIN ERROR");
				}
			}
		} catch (Exception e) {
			log.error("FTP Processing Error", e.getMessage());
		} finally {
			log.info("===================================================== FTP CLIENT RESOURCE CLEAR =====================================================");
			if (ftpClient != null) { // @formatter:off
                try { ftpClient.logout();     } catch (Exception e) { log.error("FTP Logout Error", e.getMessage()); }
                try { ftpClient.disconnect(); } catch (Exception e) { log.error("FTP Disconnect Error", e.getMessage()); }
                ftpClient = null;
            } // @formatter:on
		}

		log.info("########################################################################################################");
		return resultDataSourceList;
	}

	// FTP 파일 가져오기
	private List<SourceRecord> collectFtpFile(FTPClient ftpClient, String collectDateStr, int fileType) {

		boolean isProcessCompletePendingCommand = false;

		// "topics":
		// "l2ptntunnel,l2ptnac,l2ptnpm,l2ptnport,l2ptnpw,l2ptnoptic,l2ptntemperature"
		// "file.name.prefix":
		// "PM_TUNNEL,PM_AC,PM,PM_PORT,PM_PW,PM_OPTIC,PM_TEMPERATURE"
		List<SourceRecord> dataSourceList = null;
		String topic = String.valueOf(topics.get(fileType));
		String fileName = String.valueOf(fileNamePrefix.get(fileType)) + "_" + collectDateStr + ".txt";
		log.info("*** TARGET FILE : " + fileName + "                                               ");

		// 이미 수집 된 파일인지 확인
		try {
			FTPFile[] tmpFileList = ftpClient.listFiles(COLLECT_SUCCESS_FILE_NAME_PREFIX + fileName);
			if ((tmpFileList != null) && (tmpFileList.length == 1)) {
				log.info("ALREADY COLLECTED, CANCEL : " + fileName + "                               ");
				return null;
			}
		} catch (Exception e) {
			log.error("FTP DUPLICATE FILE CHECK ERROR", e);
		}

		BufferedReader reader = null;
		InputStreamReader isr = null;
		InputStream stream = null;
		InputStream bin = null;

		// 수집 파일 처리
		try {
			stream = ftpClient.retrieveFileStream("/" + fileName);

			if (stream != null) {
				isr = new InputStreamReader(stream, FILE_CHARSET); // MS949 로 파일 읽어야 함
				reader = new BufferedReader(isr);
				boolean headerFlag = true;
				String line, header = "";
				List<String> contents = new ArrayList<String>();

				while ((line = reader.readLine()) != null) {
					if (headerFlag) {
						header = line;
						headerFlag = false;
						continue;
					} else {
						contents.add(line);
					}
				}

				switch (fileType) {
				case 0:
					setAvroSchemaConfig("L2PTNTUNNEL");
					L2PtnTunnel l2PtnTunnel = new L2PtnTunnel(fileName, header, contents);
					l2PtnTunnel.setMessageSchma(avroSchema, avroData);
					l2PtnTunnel.setMessageValue(topic);
					dataSourceList = l2PtnTunnel.getSourceRecord();
					break;
				case 1:
					setAvroSchemaConfig("L2PTNAC");
					L2PtnAc l2PtnAc = new L2PtnAc(fileName, header, contents);
					l2PtnAc.setMessageSchma(avroSchema, avroData);
					l2PtnAc.setMessageValue(topic);
					dataSourceList = l2PtnAc.getSourceRecord();
					break;
				case 2:
					setAvroSchemaConfig("L2PTNPM");
					L2PtnPm l2PtnPm = new L2PtnPm(fileName, header, contents);
					l2PtnPm.setMessageSchma(avroSchema, avroData);
					l2PtnPm.setMessageValue(topic);
					dataSourceList = l2PtnPm.getSourceRecord();
					break;
				case 3:
					setAvroSchemaConfig("L2PTNPORT");
					L2PtnPort l2PtnPort = new L2PtnPort(fileName, header, contents);
					l2PtnPort.setMessageSchma(avroSchema, avroData);
					l2PtnPort.setMessageValue(topic);
					dataSourceList = l2PtnPort.getSourceRecord();
					break;
				case 4:
					setAvroSchemaConfig("L2PTNPW");
					L2PtnPw l2PtnPw = new L2PtnPw(fileName, header, contents);
					l2PtnPw.setMessageSchma(avroSchema, avroData);
					l2PtnPw.setMessageValue(topic);
					dataSourceList = l2PtnPw.getSourceRecord();
					break;
				case 5:
					setAvroSchemaConfig("L2PTNOPTIC");
					L2PtnOptic l2PtnOptic = new L2PtnOptic(fileName, header, contents);
					l2PtnOptic.setMessageSchma(avroSchema, avroData);
					l2PtnOptic.setMessageValue(topic);
					dataSourceList = l2PtnOptic.getSourceRecord();
					break;
				case 6:
					setAvroSchemaConfig("L2PTNTEMPERATURE");
					L2PtnTemperature l2PtnTemperature = new L2PtnTemperature(fileName, header, contents);
					l2PtnTemperature.setMessageSchma(avroSchema, avroData);
					l2PtnTemperature.setMessageValue(topic);
					dataSourceList = l2PtnTemperature.getSourceRecord();
					break;
				}

				ftpClient.completePendingCommand(); // 실행 안하면 FTP 명령, 연속 실행 못함
				isProcessCompletePendingCommand = true; // 실행 여부 체크

				// 이전에 저장된 OK 파일이 있다면 삭제
				FTPFile[] fileList = ftpClient.listFiles();
				String regEx = "^(" + COLLECT_SUCCESS_FILE_NAME_PREFIX + String.valueOf(fileNamePrefix.get(fileType)) + "_)[0-9]{8}_[0-9]{4}.txt";
				log.info("DELETE FILE NAME regEx : " + regEx);

				for (FTPFile ftpFile : fileList) {
					if (ftpFile.getName().matches(regEx)) {
						log.info("DELETE FILE NAME : " + ftpFile.getName());
						ftpClient.deleteFile(ftpFile.getName());
					}
				}

				// 파일 읽기 완료된 파일명 저장
				bin = new ByteArrayInputStream(new byte[0]);
				String tmpFileName = "/" + COLLECT_SUCCESS_FILE_NAME_PREFIX + fileName;
				ftpClient.storeFile(tmpFileName, bin);
				log.info("SAVE DUPLICATE OK FILE : " + tmpFileName);
			} else {
				log.info("### NOT EXIST TARGET FILE : " + fileName);
			}
		} catch (Exception e) {
			if (isProcessCompletePendingCommand == false) {
				try {
					ftpClient.completePendingCommand();
				} catch (IOException e1) {
					log.error("ERROR : ftpClient.completePendingCommand", e1);
				} // 실행 안하면 FTP 명령, 연속 실행 못함
			}
			log.error("TARGET FILE PROCESS ERROR", e);
		} finally {
			// @formatter:off
            log.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ COLLECT FTP FILE RESOURCE CLEAR $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            if (reader != null) { try { reader.close(); reader = null; } catch (Exception e) {} }
            if (isr != null)    { try { isr.close();    isr = null;    } catch (Exception e) {} }
            if (stream != null) { try { stream.close(); stream = null; } catch (Exception e) {} }
            if (bin != null)    { try { bin.close(); bin = null;       } catch (Exception e) {} }
            // @formatter:on
		}

		return dataSourceList;
	}

	// 수집시간 가져오기
	public String getCollectTime() {

		TimeZone tz = Calendar.getInstance().getTimeZone();
		log.info("### TIME ZONE INFO : " + tz.getDisplayName() + ", " + tz.getID());

		// 수집 시간 조정, ex) timeoffset 이 15면 15분 마다 생성되는 성능 파일을 수집
		Calendar cal = Calendar.getInstance();
		int minute = cal.get(Calendar.MINUTE); // 분 조정을 위해 분만 가져오기
		log.info("### CURRENT DATE TIME : " + DATE_FORMAT.format(cal.getTime()));

		float remain = (minute / FTP_TIME_OFFSET) - (minute / (int) FTP_TIME_OFFSET);

		if (remain == 0) { // 분이 0으로 끝나는 경우 (00, 10, 20, 30, 40, 50)
			if ((minute - (int) FTP_TIME_OFFSET) < 0) { // 00 분 일 경우 이전 시간으로 처리해야 함
				cal.add(Calendar.MINUTE, -(int) FTP_TIME_OFFSET);
			} else {
				cal.set(Calendar.MINUTE, (minute - (int) FTP_TIME_OFFSET));
			}
		} else { // 분이 0으로 끝나지 않는 경우
			cal.set(Calendar.MINUTE, ((minute / (int) FTP_TIME_OFFSET) * (int) FTP_TIME_OFFSET));
		}

		return DATE_FORMAT.format(cal.getTime());
	}

}
