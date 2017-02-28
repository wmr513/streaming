package kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Common {

	private static Properties createBaseProperties() throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}
	
	public static KafkaProducer<String, String> createStringProducer() throws Exception {
		Properties props = createBaseProperties();
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer<String, String>(props);
	}

	public static KafkaProducer<String, Long> createLongProducer() throws Exception {
		Properties props = createBaseProperties();
		props.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		return new KafkaProducer<String, Long>(props);
	}

	public static KafkaConsumer<String, String> createStringConsumer(String groupId) throws Exception {
		Properties props = createBaseProperties();
		props.put("group.id", groupId);
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new KafkaConsumer<String, String>(props);
	}

	public static KafkaConsumer<String, Long> createLongConsumer(String groupId) throws Exception {
		Properties props = createBaseProperties();
		props.put("group.id", groupId);
		props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		return new KafkaConsumer<String, Long>(props);
	}

	public static KafkaConsumer<String, Long> createLongConsumer(String groupId, int pollTimeout, int sessionTimeout, int records) throws Exception {
		Properties props = createBaseProperties();
		props.put("group.id", groupId);
		props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		if (pollTimeout > 0) {
			props.put("max.poll.interval.ms", pollTimeout);
			props.put("session.timeout.ms", sessionTimeout);
			props.put("max.poll.records", records);
		}
		return new KafkaConsumer<String, Long>(props);
	}
}
