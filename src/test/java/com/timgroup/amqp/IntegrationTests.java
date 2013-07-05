package com.timgroup.amqp;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.timgroup.concurrent.SettableFuture;

import static org.junit.Assert.assertArrayEquals;

public class IntegrationTests {
    
    private static final String TEST_BROKER_URI = "amqp://localhost";
    private Connection connection;
    private Channel channel;
    private String inboundQueueName;
    private String outboundQueueName;
    
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
    
    private String randomise(String prefix) {
        return prefix + "-" + System.currentTimeMillis();
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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    @Test
    public void aMessageSentToTheInboundQueueIsMovedToTheOutboundQueue() throws Exception {
        byte[] body = randomise("message").getBytes();
        channel.basicPublish(inboundQueueName, "", null, body);
        
        new Application().main(channel, inboundQueueName, outboundQueueName);
        
        GetResponse response = basicConsumeOnce(channel, outboundQueueName, 1, TimeUnit.SECONDS);
        
        assertArrayEquals(body, response.getBody());
    }
    
    private GetResponse basicConsumeOnce(final Channel channel, String queue, int timeout, TimeUnit unit) throws IOException,
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
