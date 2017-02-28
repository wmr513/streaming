package kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> msg = new ProducerRecord<>("customer_comment_service", "duration", "320");
		System.out.println("sending message");
		producer.send(msg);
		Thread.sleep(500); //msgs are being buffered in a separate thread!!
		producer.close();
	}
}
