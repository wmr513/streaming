package kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class ThresholdAnalyzer {

	public static void main(String[] args) throws Exception {
		boolean slow = (args.length > 0 && args[0].equals("slow")) ? true : false;
		final KafkaConsumer<String, Long> consumer = Common.createLongConsumer("G4");
		consumer.subscribe(Arrays.asList("trade_gen_service_metrics"));
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
					if (msg.key().equals("duration.req")) {
						if (slow && msg.value() > 900) {
							cnt++;
							System.out.println(msg.topic() + "(" + cnt + "): " + " response time exceeded 900ms: " + msg.value());
						} else if (!slow && msg.value() > 90) {
							cnt++;
							System.out.println(msg.topic() + "(" + cnt + "): " + " response time exceeded 90ms: " + msg.value());
						}
					}
				}
				
			}
		} catch (WakeupException w) {	
	    }  finally {
			consumer.close();
			System.out.println("consumer shutdown complete");
		}
	}
}
