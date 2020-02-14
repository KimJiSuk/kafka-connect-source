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
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class L2TL1Task extends SourceTask {

	static final Logger log = LoggerFactory.getLogger(L2TL1Task.class);

	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
	private static final Map<String, ?> SOURCE_PARTITION = Collections.emptyMap();
	private static final Map<String, ?> SOURCE_OFFSET = Collections.emptyMap();

//    private L2ParserConnectorConfig config;
	private L2TL1ConnectorConfig config;
	private List topics;
//	private List topicsWork;
//	private List fileNamePrefix;
//	private String filePath;
//	private String fileNameDateformat;
//	private String fileNameTemplete;

	private AvroFile avroFile;
	private org.apache.avro.Schema avroSchema;
	private AvroData avroData;
//	private int testIndex = 0;
//	private Calendar calendar = Calendar.getInstance();

	private String TCP_IP, TCP_CHARSET;
	private int TCP_PORT, TCP_BUFFER_SIZE, POLL_SLEEP_TIME;

	private static Socket SOCKET = null;
//	private static BufferedInputStream SOCKET_IN = null;
	private static BufferedReader SOCKET_IN = null;

	public static Object syncObj = new Object(); // 동기화 Object
	public static String recvMsg = ""; // 받은 메시지 저장 String
	public static ArrayList<String> msgQueue = new ArrayList<String>();
	public static Thread socketReadThread = null;

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
	public void start(Map<String, String> props) {
		Objects.requireNonNull(props);

		config = new L2TL1ConnectorConfig(props);
		topics = config.getTopics();
//		topicsWork = config.getTopicsWork();

		TCP_IP = config.getTcpIp();
		TCP_PORT = Integer.parseInt(config.getTcpPort());
		TCP_BUFFER_SIZE = Integer.parseInt(config.getTcpBufferSize());
		TCP_CHARSET = config.getTcpCharset();
		POLL_SLEEP_TIME = Integer.parseInt(config.getPollSleepTime());

		// TCP Client 접속 실행
		try {
			log.info("### L2 PTN FM SOCKET CONNECT TRY : IP : " + TCP_IP + ", PORT : " + TCP_PORT + ", CHARSET : " + TCP_CHARSET);
			SOCKET = new Socket(TCP_IP, TCP_PORT);
			SOCKET_IN = new BufferedReader(new InputStreamReader(SOCKET.getInputStream(), TCP_CHARSET)); // 윈도우 10 서버
			log.info("### L2 PTN FM SOCKET CONNECT SUCCESS : IP : " + TCP_IP + ", PORT : " + TCP_PORT + ", CHARSET : " + TCP_CHARSET);
		} catch (Exception e) {
			log.error("L2 PTN FM SOCKET CONNECT ERROR : " + e.getMessage());
		}

		// TCP Socket Read 실행
		socketReadThread = new Thread(new Runnable() {
			@Override
			public void run() {

				log.info("### L2 PTN SOCKET READ THREAD START");
				try {
					char[] orgRecvData = new char[TCP_BUFFER_SIZE];
					int readCnt = 0;

					while ((readCnt = SOCKET_IN.read(orgRecvData)) != -1) {
						// 데이터 받은 크기 만큼만 세팅
						String readData = new String(Arrays.copyOf(orgRecvData, readCnt));

						if (readData != null && readData.length() > 0) {
							log.info("@@@ RECV ORG DATA : [" + readData + "]");
							int idx = readData.indexOf(";"); // 메시지 종료문자 ; 확인

							// 큐에 완성된 메시지 추가
							if (idx != -1) {
								recvMsg = recvMsg + readData.substring(0, idx); // ';' 종료 문자는 뺀다.
								
								if (recvMsg.contains("EVT SESSION-CHECK") == false) { // session check 메시지는 스킵
									synchronized (syncObj) {
										msgQueue.add(recvMsg);
									}
								}
								
								/********************************************************
								* 알람 메시지 발생 테스트 - start, start ~ end 추후 삭제 해야함
								* - EVT SESSION-CHECK 메시지를 다른 메시지로 교체
								********************************************************/
								else { 
									DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
									Calendar cal = Calendar.getInstance();
									cal.setTime(new Date()); // 현재 시간 세팅
									String curDate = df.format(cal.getTime());
									cal.add(Calendar.SECOND, -3); // 알람 발생 시간 세팅
									String occureDate = df.format(cal.getTime());
									
									String tmpMsg = "   WNO3000_서초02 " + curDate + "\r\n" + "A  374 REPT ALM\r\n" + 
											"   /* AID,NAME,UNIT,REASON,SEV,SA,DATETIME */\r\n" + 
											"   SXC-P4,---,SXCUB,LINK_DOWN,CR,SA," + occureDate + "\r\n";
									synchronized (syncObj) {
										msgQueue.add(tmpMsg);
									}
									tmpMsg = "   WNO3000_동작02 " + curDate + "\r\n" + "A  377 REPT ALM\r\n" + 
											"   /* AID,NAME,UNIT,REASON,SEV,SA,DATETIME */\r\n" + 
											"   S08-P11,---,OMX24U,MODULE_OUT,CR,SA," + occureDate + "\r\n";
									synchronized (syncObj) {
										msgQueue.add(tmpMsg);
									}
								}
								/********************************************************
								* 알람 메시지 발생 테스트 - end
								********************************************************/								
								
								recvMsg = readData.substring(idx + 1); // 남은 메시지가 있으면 세팅
							} else { // 종료되지 않은 남은 메시지 계속 더하기
								recvMsg = recvMsg + readData;
							}
						}
					}
				} catch (Exception e) {
					log.error("### READ SOCKET ERROR : ", e);
				}
			}
		});

		socketReadThread.start();
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		// 수집 대기
		try {
			Thread.sleep(POLL_SLEEP_TIME); // jun test
		} catch (Exception e) {
			Thread.interrupted();
			return null;
		}
		// 리턴 결과 객체 생성
		List<SourceRecord> dataSourceList = new ArrayList<>();

		synchronized (syncObj) {
			if (msgQueue.size() > 0) {
				String topic = "";
				List<String> contents = new ArrayList<String>();
				try {
					topic = String.valueOf(topics.get(0));

					for (String orgData : msgQueue) {
						log.info("=========== READ FM QUEUE : {" + orgData + "}");
						String msg = "";
						String[] msgArray = orgData.trim().split("\r\n");

						for (String tmp : msgArray) {
							tmp = tmp.trim();
							if ("".equals(tmp) == false)
								msg += (tmp + "\n\r");
						}

						contents.add(msg);
					}
					msgQueue.clear();
				} catch (Exception e) {
					e.printStackTrace();
				}
				// AVRO 객체 생성 / Kafka 전송
				try {
					if ((contents.size() > 0) && ("".equals(topic) == false)) {
						setAvroSchemaConfig("L2PTNARM");
						L2PtnTL1 l2PtnTL1 = new L2PtnTL1(contents);
						l2PtnTL1.setMessageSchma(avroSchema, avroData);
						l2PtnTL1.setMessageValue(topic);
						dataSourceList.addAll(l2PtnTL1.getSourceRecord());
						log.info(l2PtnTL1.getSourceRecord().toString());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return dataSourceList;
	}

	@Override
	public void stop() {
		log.info("### L2 PTN FM STOP ###");
		try { // TCP Client @formatter:off
			if (SOCKET_IN        != null) { try { SOCKET_IN.close(); } catch (Exception e) {} SOCKET_IN = null; }							
			if (SOCKET           != null) { try { SOCKET.close();    } catch (Exception e) {} SOCKET = null;    }
			if (socketReadThread != null) { try { socketReadThread.interrupt(); } catch (Exception e) {} socketReadThread = null; }
			recvMsg = "";
			msgQueue.clear();
		} catch (Exception e) {} // @formatter:on
	}

// Origin source
//  @Override
//  public void start(Map<String, String> props) {
//      Objects.requireNonNull(props);
//
//      config = new L2ParserConnectorConfig(props);
//      topics = config.getTopics();
//      topicsWork = config.getTopicsWork();
//      fileNamePrefix = config.getFileNamePrefix();
//      fileNameDateformat = config.getFileNameDateformat();
//      fileNameTemplete = config.getFileNameTemplate();
//      filePath = config.getFilePath();
//
//      calendar.set(Calendar.YEAR, 2019);
//      calendar.set(Calendar.MONTH, 8);
//      calendar.set(Calendar.DATE, 27);
//      calendar.set(Calendar.HOUR_OF_DAY, 11);
//      calendar.set(Calendar.MINUTE, 15);
//  }
//
//    @Override
//    public List<SourceRecord> poll() throws InterruptedException {
//
//        try {
//            Thread.sleep((long) (1000 * Math.random()));
//        } catch (InterruptedException e) {
//            Thread.interrupted();
//            return null;
//        }
//
//        log.info("testIndex : " + String.valueOf(testIndex));
//
//        List<SourceRecord> dataSourceList = new ArrayList<>();
//
//        for (int i = 0; i < topics.size(); i++) {
//            String topic = String.valueOf(topics.get(i));
//            String prefix = String.valueOf(fileNamePrefix.get(i));
//
//            String fileName = filePath + getFileName(prefix);
//
//            log.info("---------------------------------------");
//            log.info("FileName : " + fileName);
//            log.info("---------------------------------------");
//
//            File file = new File(fileName);
//            final List<SourceRecord> records = new ArrayList<>();
//
//            try {
//                FileReader fileReader = new FileReader(file);
//                BufferedReader bufferedReader = new BufferedReader(fileReader);
//
//                String line;
//                String content = "";
//                List<String> contents = new ArrayList<String>();
//
//                while((line = bufferedReader.readLine()) != null) {
//                    content += (line + "\n\r");
//
//                    if (line.trim().equals(";")) {
//                        content = content.replace(content.substring(content.length() - 1), "");
//                        contents.add(content);
//                        content = "";
//                    }
//                }
//
//                setAvroSchemaConfig("L2PTNARM");
//                L2PtnTL1 l2PtnTL1 = new L2PtnTL1(fileName, contents);
//                l2PtnTL1.setMessageSchma(avroSchema, avroData);
//
//                log.info("finish1");
//                l2PtnTL1.setMessageValue(topic);
//
//                log.info("finish2");
//                dataSourceList.addAll(l2PtnTL1.getSourceRecord());
//
//                log.info(l2PtnTL1.getSourceRecord().toString());
//
//                testIndex++;
//
//                bufferedReader.close();
//
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ParserConfigurationException e) {
//                e.printStackTrace();
//            } catch (XPathExpressionException e) {
//                e.printStackTrace();
//            } catch (SAXException e) {
//                e.printStackTrace();
//            }
//        }
//
//        log.info("finish");
//
//        return dataSourceList;
//    }
//
//	private String getFileName(String prefix) {
//		SimpleDateFormat dateFormat = new SimpleDateFormat(fileNameDateformat);
//		String timeString = dateFormat.format(calendar.getTime());
//
//		Template template = new Template(fileNameTemplete);
//
//		return template.instance().bindVariable("prefix", () -> prefix).bindVariable("dateformat", () -> timeString).render();
//	}

}
