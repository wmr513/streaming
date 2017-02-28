package kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

import common.AMQPCommon;

@SuppressWarnings("unchecked")
public class TradeValidator {

	private static long delay = 0; 
	private String serviceName = "trade_validate_service";
	private int errorIndex = 0;
	private int tradeError = ((int) ((new Random().nextDouble() * 5)));
	private KafkaProducer<String, Long> metricsProducer;

	public void execute() throws Exception {
		Channel channel = AMQPCommon.connect();
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume("trade.validation.request.q", true, consumer);
		metricsProducer = Common.createLongProducer();
		
		while (true) {
			QueueingConsumer.Delivery msg = consumer.nextDelivery();
			String message = new String(msg.getBody());
			System.out.println("validating trade: " + message);
			Thread.sleep(delay);
			byte[] rmessage = String.valueOf(isValid()).getBytes();
			channel.basicPublish("", "trade.validation.response.q", null, rmessage);
			stream(metricsProducer, "metrics", null);
		}			
	}	
	
	public static void main(String[] args) throws Exception {
		delay = (args.length > 0 && args[0].equals("slow")) ? 300 : 30;
		new TradeValidator().execute();
	}

	@SuppressWarnings("rawtypes")
	private void stream(KafkaProducer producer, String type, String msg) throws Exception {
		if (type.equals("metrics")) {
			List<Long> metrics = generateMetrics();
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "duration.req", metrics.get(0)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "duration.min", metrics.get(1)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "duration.max", metrics.get(2)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "variance.req", metrics.get(3)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "variance.min", metrics.get(4)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "variance.max", metrics.get(5)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "stddev.req", metrics.get(6)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "stddev.min", metrics.get(7)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "stddev.max", metrics.get(8)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "duration.90th", metrics.get(9)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "duration.95th", metrics.get(10)));
			producer.send(new ProducerRecord<>(serviceName + "_metrics", "duration.99th", metrics.get(11)));
		}
	}
	
	private List<Long> generateMetrics() {
		List<Long> metrics = Arrays.asList(
				((long) ((new Random().nextDouble() * delay) + 1)),
				((long) ((new Random().nextDouble() * 200) + 1)),
				((long) ((new Random().nextDouble() * 300) + 1)),
				((long) ((new Random().nextDouble() * 200) + 1)),
				((long) ((new Random().nextDouble() * 100) + 1)),
				((long) ((new Random().nextDouble() * 300) + 1)),
				((long) ((new Random().nextDouble() * 99) + 1)),
				((long) ((new Random().nextDouble() * 5) + 1)),
				((long) ((new Random().nextDouble() * 99) + 1)),
				((long) ((new Random().nextDouble() * 100) + 1)),
				((long) ((new Random().nextDouble() * 200) + 1)),
				((long) ((new Random().nextDouble() * 300) + 1)));
		return metrics;
	}
	
	boolean isValid() {
		boolean valid = (errorIndex != tradeError);
		if (errorIndex > 5) {
			errorIndex = 0;
			tradeError = ((int) ((new Random().nextDouble() * 5)));
		} else {
			errorIndex = errorIndex+1;
		}
		return valid;
	}

}
