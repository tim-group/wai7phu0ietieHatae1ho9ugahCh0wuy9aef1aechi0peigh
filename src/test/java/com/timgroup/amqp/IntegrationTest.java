package com.timgroup.amqp;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.timgroup.concurrent.SettableFuture;

public abstract class IntegrationTest {
    
    protected static final Random RANDOM = new Random();
    public static final String TEST_BROKER_HOST = "localhost";
    public static final String TEST_BROKER_URI = "amqp://" + TEST_BROKER_HOST;
    public static final String TEST_BROKER_USERNAME = "guest"; // that's who you are if you don't use an explicit name, apparently
    
    protected Connection connection;
    protected Channel channel;
    protected String inboundQueueName;
    protected String outboundQueueName;
    
    @Before
    public void setUp() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(TEST_BROKER_URI);
        connection = factory.newConnection();
        channel = connection.createChannel();
        inboundQueueName = configureSimpleQueue("inbound");
        outboundQueueName = configureSimpleQueue("outbound");
    }
    
    private String configureSimpleQueue(String prefix) throws IOException {
        String queueName = randomise(prefix);
        configureSimpleQueue(channel, queueName);
        return queueName;
    }
    
    protected String randomise(String prefix) {
        return prefix + "-" + Long.toHexString(RANDOM.nextLong());
    }
    
    private static void configureSimpleQueue(Channel channel, String queueName) throws IOException {
        // easiest way to have a one-to-one exchange-to-queue setup is to use a fanout to one destination
        boolean durable = true; // write the messages to disk
        boolean autoDelete = false; // but don't keep the queues over a server restart
        channel.exchangeDeclare(queueName, "fanout", durable, autoDelete, null);
        channel.queueDeclare(queueName, durable, false, autoDelete, null);
        // fanout exchanges ignore the routing key, so use the empty string
        channel.queueBind(queueName, queueName, "");
    }
    
    @After
    public void tearDown() {
        // neither channel or connection are Closeable, yay
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    protected GetResponse basicConsumeOnce(final Channel channel, String queue, int timeout, TimeUnit unit) throws IOException,
            InterruptedException, ExecutionException, TimeoutException {
        final SettableFuture<GetResponse> future = new SettableFuture<GetResponse>();
        
        channel.basicConsume(queue, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                GetResponse response = new GetResponse(envelope, properties, body, -1);
                future.set(response);
                channel.basicCancel(consumerTag);
            }
        });
        
        return future.get(timeout, unit);
    }
    
}
