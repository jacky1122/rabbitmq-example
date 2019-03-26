package com.demo.rabbitmq;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * RabbitmqExample2ExchangeConsumTest
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitmqExample2ExchangeConsumTest {
    @Test
    public void testBasicConsumer() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(AMQP.PROTOCOL.PORT);
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        channel.basicQos(1);

        String queueName = "queue.work";
        channel.queueDeclare(queueName, true, false, false, null);
        System.out.println("Consumer waiting receive message");

        Consumer consumer = new DefaultConsumer(channel) {
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                    byte[] body) throws UnsupportedEncodingException {
                String message = new String(body, "UTF-8"); 
                try{
                    doWork(message); 
                    channel.basicAck(envelope.getDeliveryTag(), false); 
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }; 

        channel.basicConsume(queueName, false, consumer); 

        Thread.sleep(1000000); 
    }
    
    private void doWork(String message) throws Exception {
        System.out.println("[C]Received '"+message+"', 业务处理中...'");
        Thread.sleep(1000);
    }

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        new RabbitmqExample2ExchangeConsumTest().testBasicConsumer();
    }
}