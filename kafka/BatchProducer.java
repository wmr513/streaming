package kafka;

import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BatchProducer {

	public static void main(String[] args) throws Exception {
		KafkaProducer<String, Long> producer = Common.createLongProducer();
		while (true) {
			for (int i=0;i<10;i++) {
				long duration = ((long) ((new Random().nextDouble() * 500) + 1));
				ProducerRecord<String, Long> msg = new ProducerRecord<>("customer_comment_service", "duration", duration);
				System.out.println("sending duration: " + duration);
				producer.send(msg);
			}
			Thread.sleep(3000); 
		}
	}
}
