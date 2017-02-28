package kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class MetricsAnalyzer {

	public static void main(String[] args) throws Exception {
		final KafkaConsumer<String, Long> consumer = Common.createLongConsumer("G1");
		consumer.subscribe(Arrays.asList("trade_gen_service_metrics", "trade_validate_service_metrics"));
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
			long cnt = 0;
			while (true) {
				ConsumerRecords<String, Long> msgs = consumer.poll(100);
				for (ConsumerRecord<String, Long> msg : msgs) {
					cnt++;
					System.out.println(msg.topic() + "(" + cnt + "): " + ": " + msg.key() + " = " + msg.value());
				}
				
			}
		} catch (WakeupException w) {	
	    }  finally {
			consumer.close();
			System.out.println("consumer shutdown complete");
		}
	}
}
