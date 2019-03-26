package com.demo.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * RabbitmqExample2Test
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitmqExample2ExchangePublishTest {

    @Test
    public void testBasicPublish() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(AMQP.PROTOCOL.PORT);
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queueName = "queue.work";
        String routingKey = "task";
        String exchangeName = "amqp.rabbitmq.work";

        channel.queueDeclare(queueName, true, false, false, null);
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
        channel.queueBind(queueName, exchangeName, routingKey); 

        for(int i = 0; i < 10; i++){
            String message = "Hello rabbitMQ " + i; 
            channel.basicPublish(exchangeName, routingKey, null, message.getBytes("UTF-8")); 
        }

        channel.close(); 
        connection.close(); 
    }


    public static void main(String[]args) throws IOException, TimeoutException{
        new RabbitmqExample2ExchangePublishTest().testBasicPublish();
    }

    
}

