package kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BatchConsumer {

	public static void main(String[] args) throws Exception {
		final KafkaConsumer<String, Long> consumer = Common.createLongConsumer("T1");
		consumer.subscribe(Arrays.asList("customer_comment_service"));		

		try {
			System.out.println("waiting for messages...");
			while (true) {
				ConsumerRecords<String, Long> msgs = consumer.poll(1000);
				System.out.println("poll successful msg count = " + msgs.count());
				for (ConsumerRecord<String, Long> msg : msgs) {
					System.out.println("processing duration: " + msg.value());
					Thread.sleep(100);
				}				
			}
	    }  finally {
			consumer.close();
			System.out.println("consumer shutdown complete");
		}
	}

}
