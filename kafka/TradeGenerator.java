package kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import common.AMQPCommon;

@SuppressWarnings("unchecked")
public class TradeGenerator {

	private static long delay = 0;
	private String serviceName = "trade_gen_service";
	private List<String> symbols = Arrays.asList("IBM", "AAPL", "GOOG", "AAPL", "ATT", "AAPL", "ATT");
	private Connection connection = null;
	private KafkaProducer<String, Long> metricsProducer;
	private KafkaProducer<String, String> errorProducer;
	private KafkaProducer<String, String> symbolProducer;

	public static void main(String[] args) throws Exception {
		delay = (args.length > 0 && args[0].equals("slow")) ? 700 : 70;
		TradeGenerator app = new TradeGenerator();
		app.connection = AMQPCommon.connect().getConnection();
		app.produceMessages();
	}
	
	private void produceMessages() throws Exception {		
		metricsProducer = Common.createLongProducer();
		errorProducer = Common.createStringProducer();
		symbolProducer = Common.createStringProducer();
		
		while (true) {
			int index = ((int) ((new Random().nextDouble() * 7)));
			String symbol = symbols.get(index);
			stream(symbolProducer, "symbol", symbol);
			long shares = ((long) ((new Random().nextDouble() * 4000) + 1));
			String trade = "BUY " + symbol + " " + shares + " SHARES";
			Thread.sleep(delay);
			if (validateMessage(trade)) {
				System.out.println("placing trade: " + trade);
			} else {
				System.out.println("trade error: " + trade);
				stream(errorProducer, "error", "trade error: " + trade);
			}
			stream(metricsProducer, "metrics", null);
		}
	}
	
	private boolean validateMessage(String trade) throws Exception {
		Channel channel = connection.createChannel();
		byte[] message = trade.getBytes();
		BasicProperties props = new BasicProperties
			.Builder().replyTo("trade.validation.response.q").build();		
		channel.basicPublish("", "trade.validation.request.q", props, message);

		final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        channel.basicConsume("trade.validation.response.q", true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, 
            		AMQP.BasicProperties properties, byte[] body) throws IOException {
            response.offer(new String(body, "UTF-8"));
            }
        });
		boolean valid = new Boolean(new String(response.take())).booleanValue();
		channel.close();
		return valid;
	}
	
	@SuppressWarnings("rawtypes")
	private void stream(KafkaProducer producer, String type, String msg) throws Exception {
		if (type.equals("error")) {
			producer.send(new ProducerRecord<>(serviceName + "_errors", "error", msg));
		} else if (type.equals("metrics")) {
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
		} else if (type.equals("symbol")) {
			producer.send(new ProducerRecord<>(serviceName + "_symbol", "symbol", msg));
		}
	}
	
	private List<Long> generateMetrics() {
		long durationTotal = (delay == 700) ? 1000 : 100;
		List<Long> metrics = Arrays.asList(
				((long) ((new Random().nextDouble() * durationTotal) + 1)),
				((long) ((new Random().nextDouble() * 1000) + 1)),
				((long) ((new Random().nextDouble() * 4000) + 1)),
				((long) ((new Random().nextDouble() * 4000) + 1)),
				((long) ((new Random().nextDouble() * 1000) + 1)),
				((long) ((new Random().nextDouble() * 4000) + 1)),
				((long) ((new Random().nextDouble() * 99) + 1)),
				((long) ((new Random().nextDouble() * 5) + 1)),
				((long) ((new Random().nextDouble() * 99) + 1)),
				((long) ((new Random().nextDouble() * 600) + 1)),
				((long) ((new Random().nextDouble() * 800) + 1)),
				((long) ((new Random().nextDouble() * 1000) + 1)));
		return metrics;
	}
}





