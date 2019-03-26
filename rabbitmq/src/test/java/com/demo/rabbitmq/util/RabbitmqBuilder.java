package com.demo.rabbitmq.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

import org.springframework.util.StringUtils;

/**
 * RabbitMqBuilder
 */
public class RabbitmqBuilder {
    private String host;
    private int port;
    private String userName;
    private String password;

    public static RabbitmqBuilder builder() {
        return new RabbitmqBuilder();
    }

    public RabbitmqBuilder host(String host) {
        this.host = host;
        return this;
    }

    public String host() {
        return this.host;
    }

    public RabbitmqBuilder port(int port) {
        this.port = port;
        return this;
    }

    public int port() {
        return this.port;
    }

    public String userName() {
        return this.userName;
    }

    public RabbitmqBuilder userName(String userName) {
        this.userName = userName;
        return this;
    }

    public String password() {
        return this.password;
    }

    public RabbitmqBuilder password(String password) {
        this.password = password;
        return this;
    }

    public FactoryBuilder factory() {
        return new FactoryBuilder(this);
    }

    public ChannelBuilder channel() throws IOException, TimeoutException {
        return factory().connection().channel(); 
    }

    public static class FactoryBuilder {
        private ConnectionFactory factory;

        public FactoryBuilder(RabbitmqBuilder mqBuilder) {
            factory = new ConnectionFactory();
            factory.setHost(StringUtils.isEmpty(mqBuilder.host()) ? "127.0.0.1" : mqBuilder.host());
            factory.setPort(mqBuilder.port() <= 0 ? AMQP.PROTOCOL.PORT : mqBuilder.port());
            factory.setUsername(StringUtils.isEmpty(mqBuilder.userName()) ? "guest" : mqBuilder.userName());
            factory.setPassword(StringUtils.isEmpty(mqBuilder.password()) ? "guest" : mqBuilder.password());
        }

        public ConnectionBuilder connection() throws IOException, TimeoutException {
            return new ConnectionBuilder(factory.newConnection());
        }
    }

    public static class ConnectionBuilder {
        private Connection connection ; 
        public ConnectionBuilder(Connection connection){
            this.connection = connection; 
        }

        
        public ChannelBuilder channel() throws IOException {
            return new ChannelBuilder(connection.createChannel(), this);
        }

        public void close() throws IOException {
            this.connection.close(); 
        }
    }

    public static class ChannelBuilder {
        private Channel channel; 
        private ConnectionBuilder connectionBuilder; 
        public ChannelBuilder(Channel channel, ConnectionBuilder connectionBuilder ){
            this.channel = channel;
            this.connectionBuilder = connectionBuilder ; 
        }
    
        public QueueBuilder queue(){
            return new QueueBuilder(channel, this);
        }

        public void close() throws IOException, TimeoutException{
            this.channel.close(); 
            connectionBuilder.close(); 
        }

        
        public ExchangeBuilder exchangeBuilder(){
            //return new ExchangeBuilder(this.channel, this); 
            return null;
        }
    }

    public static class QueueBuilder{
        /**
         * 队列名称
         */
        private String queueName; 
        /**
         * 是否持久化,默认不持久化
         */
        private boolean durable = false; 
        /**
         * 是否独占队列，创建者使用的私有队列，端口后自动删除，默认非独占
         */
        private boolean exclusive = false; 
        /**
         * 是否自动删除，所有消费者客户端断开时自动删除队列，默认不自动删除
         */
        private boolean autoDelete = false; 
        /**
         * 队列的其他参数
         */
        private Map<String, Object> arguments; 
        
        private Channel channel ; 

        private ChannelBuilder channelBuilder;
        public QueueBuilder(Channel channel, ChannelBuilder channelBuilder){
            this.channel = channel; 
            this.channelBuilder = channelBuilder; 
        }

        public QueueBuilder queueName(String queueName){
            this.queueName = queueName;
            return this; 
        }
        public String queueName(){
            return this.queueName;
        }

        public QueueBuilder durable(boolean durable){
            this.durable = durable; 
            return this; 
        }

        public boolean durable(){
            return this.durable; 
        }

        public QueueBuilder exclusive(boolean exclusive){
            this.exclusive = exclusive; 
            return this; 
        }

        public boolean exclusive(){
            return this.exclusive; 
        }

        public QueueBuilder autoDelete(boolean autoDelete){
            this.autoDelete = autoDelete; 
            return this; 
        }

        public boolean autoDelete(){
            return this.autoDelete;
        }

        public Map<String, Object> arguments(){
            if(this.arguments == null){
                return null; 
            }
            return Collections.unmodifiableMap(this.arguments);
        }

        public QueueBuilder arguments(Map<String, Object> arguments){
            this.arguments = arguments; 
            return this; 
        }

        public QueueBuilder addArgument(String key, Object value){
            if(this.arguments ==null){
                this.arguments = new HashMap<String, Object>(); 
            }
            this.arguments.put(key, value);
            return this; 
        }

        public Object removeArgument(String key){
            if(this.arguments == null){
                return false; 
            }
            return this.arguments.remove(key);
        }

        public QueueBuilder declare() throws IOException {
            channel.queueDeclare(this.queueName, this.durable, this.exclusive, 
                this.autoDelete, this.arguments); 
            return this; 
        }

        public PublisherBuilder publisher(){
            return new PublisherBuilder(this.channel, this); 
        }

        public PublisherBuilder publisher(String queueName) throws IOException {
            return queueName(queueName).declare().publisher();
        }

        public ConsumerBuilder consumer(){
            return new ConsumerBuilder(this.channel, this); 
        }

        public ConsumerBuilder consumer(String queueName) throws IOException {
            return queueName(queueName).declare().consumer();
        }
    }

    public static class ExchangeBuilder{
        private Channel channel; 
        private ConnectionBuilder connectionBuilder ; 
        public ExchangeBuilder(Channel channel, ConnectionBuilder connectionBuilder){
            this.channel = channel ; 
            this.connectionBuilder = connectionBuilder; 
        
            //this.channel.exchangeDeclare(exchange, type)
        }


    }

    public static class ConsumerBuilder{
        private Consumer consumer; 
        private Channel channel ; 
        private QueueBuilder queueBuilder; 
        private String queueName; 
        private boolean autoAck; 

        public ConsumerBuilder(Channel channel, QueueBuilder queueBuilder){
            this.channel = channel ; 
            this.queueBuilder = queueBuilder; 
            this.queueName = queueBuilder.queueName(); 
        }

        public ConsumerBuilder consumer(Consumer consumer){
            this.consumer = consumer; 
            return this; 
        }

        public ConsumerBuilder consumer(IMessageConsumer messageConsumer){
            Consumer consumer = new DefaultConsumer(this.channel) {

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                        byte[] body) throws IOException {
                    messageConsumer.consume(consumerTag, envelope, properties, body);			
                }
            };
            this.consumer = consumer; 
            return this; 
        }

        public Consumer consumer(){
            return this.consumer; 
        }

        public ConsumerBuilder queueName(String queueName){
            this.queueName = queueName; 
            return this; 
        }

        public String queueName(){
            return this.queueName; 
        }

        public ConsumerBuilder autoAck(boolean autoAck){
            this.autoAck = autoAck ; 
            return this; 
        }

        public boolean autoAck(){
            return this.autoAck; 
        }

        public ConsumerBuilder consume() throws IOException {
            this.channel.basicConsume(this.queueName, this.autoAck, consumer);
            return this; 
        }
    }

    
	public interface IMessageConsumer{
		public void consume(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
		    byte[] body) throws IOException;
	}

    public static class PublisherBuilder{
        private Channel channel ; 
        private QueueBuilder queueBuilder; 
        private String exchange = ""; 
        private String routingKey; 
        private byte[] message; 
        private BasicProperties props; 
        public PublisherBuilder(Channel channel, QueueBuilder queueBuilder){
            this.channel = channel; 
            this.queueBuilder = queueBuilder; 
            this.routingKey = queueBuilder.queueName(); 
        }

        public PublisherBuilder exchange(String exchange){
            this.exchange = exchange; 
            return this; 
        }

        public String exchange(){
            return this.exchange; 
        }

        public PublisherBuilder routingKey(String routingKey){
            this.routingKey = routingKey; 
            return this; 
        }

        public String routingKey(){
            return this.routingKey; 
        }

        public PublisherBuilder message(byte[] message){
            this.message = message; 
            return this; 
        }

        public PublisherBuilder message(String message){
            this.message = message.getBytes(Charset.forName("UTF-8"));
            return this;
        }
        public byte[] message(){
            return this.message; 
        }

        public BasicPropertiesBuilder  props(){
            return new BasicPropertiesBuilder(this); 
        }

        public PublisherBuilder props(BasicProperties props){
            this.props = props; 
            return this; 
        }

        public QueueBuilder publish() throws IOException {
            channel.basicPublish(StringUtils.isEmpty(this.exchange) ? "":this.exchange, 
                this.routingKey, this.props, this.message); 
            return this.queueBuilder; 
        }
    }

    public static class BasicPropertiesBuilder{
        private PublisherBuilder publisherBuilder;
        
        private String contentType;
        private String contentEncoding;
        private Map<String,Object> headers;
        private Integer deliveryMode;
        private Integer priority;
        private String correlationId;
        private String replyTo;
        private String expiration;
        private String messageId;
        private Date timestamp;
        private String type;
        private String userId;
        private String appId;
        private String clusterId;


        
        public BasicPropertiesBuilder(PublisherBuilder publisherBuilder){
            this.publisherBuilder = publisherBuilder; 
        }   

        public PublisherBuilder end(){
            this.publisherBuilder.props(this.build());
            return this.publisherBuilder; 
        }

        public BasicPropertiesBuilder contentType(String contentType){
            this.contentType = contentType; 
            return this; 
        }

        public BasicPropertiesBuilder contentEncoding(String contentEncoding){
            this.contentEncoding = contentEncoding; 
            return this; 
        }

        public BasicPropertiesBuilder headers(Map<String, Object> headers)  {
            this.headers = headers; 
            return this; 
        }

        public BasicPropertiesBuilder priority(Integer priority) {
            this.priority = priority; 
            return this; 
        }
        public BasicPropertiesBuilder correlationId(String correlationId) {
            this.correlationId = correlationId; 
            return this; 
        }
        public BasicPropertiesBuilder replyTo(String replyTo) {
            this.replyTo = replyTo; 
            return this; 
        }
        public BasicPropertiesBuilder expiration(String expiration) {
            this.expiration = expiration; 
            return this; 
        }
        public BasicPropertiesBuilder messageId(String messageId) {
            this.messageId = messageId; 
            return this; 
        }
        public BasicPropertiesBuilder timestamp(Date timestamp) {
            this.timestamp = timestamp; 
            return this; 
        }
        public BasicPropertiesBuilder type(String type) {
            this.type = type; 
            return this; 
        }
        public BasicPropertiesBuilder userId(String userId) {
            this.userId = userId; 
            return this; 
        }
        public BasicPropertiesBuilder appId(String appId) {
            this.appId = appId; 
            return this; 
        }
        public BasicPropertiesBuilder clusterId(String clusterId) {
            this.clusterId = clusterId; 
            return this; 
        }

        public String contentType(){
            return this.contentType; 
        }
        public String contentEncoding(){
            return this.contentEncoding; 
        }
        public Map<String, Object> headers(){
            return this.headers; 
        }
        public Integer deliveryMode(){
            return this.deliveryMode; 
        }
        public Integer priority(){
            return this.priority; 
        }

        public String correlationId(){
            return this.correlationId; 
        }
        public String replyTo(){
            return this.replyTo; 
        }
        public String expiration(){
            return this.expiration; 
        }
        public String messageId(){
            return this.messageId; 
        }
        public Date timestamp(){
            return this.timestamp; 
        }
        public String type(){
            return this.type; 
        }
        public String userId(){
            return this.userId; 
        }
        public String appId(){
            return this.appId; 
        }
        public String clusterId(){
            return this.clusterId; 
        }
        public BasicProperties build(){
            return new BasicProperties(this.contentType, this.contentEncoding, 
                this.headers, this.deliveryMode, this.priority, 
                this.correlationId, this.replyTo, this.expiration, 
                this.messageId, this.timestamp, this.type, this.userId, 
                this.appId, this.clusterId); 
        }
    }
}