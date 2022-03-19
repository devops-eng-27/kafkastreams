package test.kafka.stream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamApplication {

	private static final Logger logger = LoggerFactory.getLogger(KafkaStreamApplication.class);
	
	private static KafkaStreams streams;

	private static String appId = "test-kafka"; // app id명
	private static String BOOTSTRAP_SERVERS = "localhost:9092"; // 카프카 클러스터 주소

	public static void main(String[] args) throws Exception{
		SpringApplication.run(KafkaStreamApplication.class, args);
		
		airmapProducer("SRC");
		func_QC("SRC", "PRO", "SINK1", "SINK2", "SINK3");
	}

	public static Properties getKafkaStreamsProperties(String appId) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

		return props;
	}

	public static void func_QC(String topicSource, String topicProcess, String topicSink1, String topicSink2, String topicSink3) {
		
		
		KStreamBuilder builder = new KStreamBuilder();
		KStream<byte[], byte[]> source = builder.stream(Serdes.ByteArray(), Serdes.ByteArray(), topicSource);

		source.flatMap((key, value) -> ParseJson.getKeyValue(value))
				.to(Serdes.String(), Serdes.String(), topicProcess);

		KStream<String, String> process = builder.stream(Serdes.String(), Serdes.String(),topicProcess);
		
		@SuppressWarnings("unchecked")
		KStream<String, String>[] sourceBranch = process.branch(
				(key, value) -> key.substring(key.length()-1,key.length()).equals("0"),
				(key, value) -> key.substring(key.length()-1,key.length()).equals("1"),
				(key, value) -> key.substring(key.length()-1,key.length()).equals("2")
		);
		
		sourceBranch[0].flatMap((key,value) -> ChangeValue.reValue("1", value))
						.to(Serdes.String(), Serdes.String(), topicSink1);
		
		sourceBranch[1].flatMap((key,value) -> ChangeValue.reValue("2", value))
						.mapValues(value -> value="good") 
						.to(Serdes.String(), Serdes.String(), topicSink2);
		
		sourceBranch[2].flatMap((key,value) -> ChangeValue.reValue("3", value))
						.filter((key,value) -> value.equals("3"))
						.to(Serdes.String(), Serdes.String(), topicSink3);
		
		TopologyBuilder topology = new TopologyBuilder();
		topology = builder;

		streams = new KafkaStreams(topology, getKafkaStreamsProperties(appId));
		final CountDownLatch latch = new CountDownLatch(1);

		try {
			streams.start();
			latch.await();
		} catch (final Throwable e) {
			System.exit(1);
		}

		Runtime.getRuntime().addShutdownHook(new Thread("test-consumer-group") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		System.exit(0);

	}

	// Producer
	public static void airmapProducer(String topicSink) {

		// properties 추가
		Properties configs = new Properties();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		// producer 생성
		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configs);

		for (Double i=6.0; i <15.0; i++) {
			
		String jsonForm = "{" + "\"Date\":" + "\"" + "2022-01-13 13:55:44.000" + "\"" + ","
				+ "\"Data\": " + "{" + "\"1\":12.8," + "\"2\":" + i + ","
				+ "\"3\":" + (i+1) + "," + "\"4\":0.0," + "\"5\":" + i + "}" + "}";

		byte[] jsonByte = jsonForm.getBytes();

		ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicSink, jsonByte);
	        
		try {
			producer.send(record);
			Thread.sleep(100);
		} catch (Exception e) {
			System.out.println(e);
		}
	}
		producer.close();

	}
}
