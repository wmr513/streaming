package kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class SymbolAnalyzer {

	public static void main(String[] args) throws Exception {
		Map<String, Long> symbolCount = new HashMap<String, Long>();
		symbolCount.put("AAPL", new Long(0));
		symbolCount.put("IBM", new Long(0));
		symbolCount.put("GOOG", new Long(0));
		symbolCount.put("ATT", new Long(0));

		final KafkaConsumer<String, String> consumer = Common.createStringConsumer("G3");
		consumer.subscribe(Collections.singletonList("trade_gen_service_symbol"));
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
					symbolCount.put(msg.value(), symbolCount.get(msg.value())+1);
					System.out.println(
							"AAPL=" + symbolCount.get("AAPL") + 
							", GOOG=" + symbolCount.get("GOOG") + 
							", IBM=" + symbolCount.get("IBM") + 
							", ATT=" + symbolCount.get("ATT") +
							", TOTAL=" + (symbolCount.get("ATT")+symbolCount.get("AAPL")+symbolCount.get("GOOG")+symbolCount.get("ATT")));
				}
			}
		} catch (WakeupException w) {	
	    }  finally {
			consumer.close();
			System.out.println("consumer shutdown complete");
		}
	}
}
