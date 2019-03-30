package com.demo.rabbitmq;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

import com.demo.rabbitmq.util.RabbitmqBuilder;
import com.demo.rabbitmq.util.RabbitmqBuilder.ChannelBuilder;
import com.demo.rabbitmq.util.RabbitmqBuilder.IMessageConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * RabbitmqExample1Basic
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitmqExample1BasicTest {
    
	@Test
	public void testBasicPublish() throws IOException, TimeoutException {
		/* ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("127.0.0.1");
		factory.setPort(AMQP.PROTOCOL.PORT); 
		factory.setUsername("guest");
		factory.setPassword("guest");

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		String queueName = "hello";
		channel.queueDeclare(queueName, false, false, false, null);

		String message = "Hello RabbitMQ!";
		channel.basicPublish("", queueName, null, message.getBytes(Charset.forName("UTF-8")));
		System.out.println("Producer send a message: " + message);
		channel.close();
		connection.close(); */

        ChannelBuilder channelBuilder = RabbitmqBuilder.builder()
            .host("127.0.0.1")
            .port(AMQP.PROTOCOL.PORT)
            .userName("guest")
			.password("guest")
			.channel(); 
						
		channelBuilder.queue()
			.publisher("hello")
				.message("Hello RabbitMQ!")
				.publish();

		System.out.println("Producer send a message: Hello RabbitMQ!");
		channelBuilder.close();
	}

	@Test
	public void testBasicConsumer() throws IOException, TimeoutException {
		/* ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("127.0.0.1");
		factory.setPort(AMQP.PROTOCOL.PORT);

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		String queueName = "hello";

		channel.queueDeclare(queueName, false, false, false, null);
		System.out.println("Consumer waiting receive message");
		Consumer consumer = new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, Charset.forName("UTF-8"));
				System.out.println("[C]Received message: " + message);			
			}
		};
		channel.basicConsume(queueName, true, consumer); */

		ChannelBuilder channelBuilder = RabbitmqBuilder.builder()
            .host("127.0.0.1")
            .port(AMQP.PROTOCOL.PORT)
            .userName("guest")
			.password("guest")
			.channel(); 
						
		channelBuilder.queue()
			.consumer("hello")
			.autoAck(true)
			.consumer(new IMessageConsumer(){
				public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException{
					String message = new String(body, Charset.forName("UTF-8"));
					System.out.println("[C]Received message: " + message);
				}
			})
			.consume();

		System.out.println("Consumer consume a message: Hello RabbitMQ!");
		channelBuilder.close();
	}

}