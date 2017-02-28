package kafka;

import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class SimpleConsumer {

	public static void main(String[] args) throws Exception {
		KafkaConsumer<String, String> consumer = Common.createStringConsumer("T1");
		consumer.subscribe(Arrays.asList("customer_comment_service"));		

		final Thread threadMain = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println("shutting down consumer...");
				consumer.wakeup();
				try {
					threadMain.join();
				} catch (Exception e) {e.printStackTrace();}
			}
		});
		
		try {
			System.out.println("waiting for messages...");
			while (true) {
				ConsumerRecords<String, String> msgs = consumer.poll(100);
				for (ConsumerRecord<String, String> msg : msgs) {
					System.out.println("topic: " + msg.topic()); 
					System.out.println("key: " + msg.key());
					System.out.println("value: " + msg.value());
					System.out.println("partition: " + msg.partition());
					System.out.println("offset: " + msg.offset());
				}				
			}
		} catch (WakeupException w) {	
	    }  finally {
			consumer.close();
			System.out.println("consumer shutdown complete");
		}
	}
}
