package com.timgroup.amqp;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public abstract class IntegrationTestBase {
    
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
        if (connection != null) {
            Application.closeQuietly(Application.closeable(connection));
        }
    }
    
    protected GetResponse basicConsumeOnce(final Channel channel, String queue, int timeout, TimeUnit unit) throws IOException, InterruptedException {
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queue, true, consumer);
        Delivery delivery = consumer.nextDelivery(unit.toMillis(timeout));
        channel.basicCancel(consumer.getConsumerTag());
        return new GetResponse(delivery.getEnvelope(), delivery.getProperties(), delivery.getBody(), 1);
    }
    
}
