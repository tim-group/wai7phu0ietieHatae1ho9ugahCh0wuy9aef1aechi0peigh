package com.timgroup.amqp;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Receiver implements Closeable {
    
    public static final String SCHEDULED_DELIVERY_HEADER = "scheduled_delivery";
    
    private final Channel channel;
    private final String inboundQueueName;
    private final String outboundQueueName;
    private final ScheduledExecutorService executor;
    
    public Receiver(Channel channel, String inboundQueueName, String outboundQueueName) {
        this.channel = channel;
        this.inboundQueueName = inboundQueueName;
        this.outboundQueueName = outboundQueueName;
        executor = Executors.newSingleThreadScheduledExecutor();
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public String getInboundQueueName() {
        return inboundQueueName;
    }
    
    public String getOutboundQueueName() {
        return outboundQueueName;
    }
    
    public void start() throws IOException {
        channel.basicConsume(inboundQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, final Envelope envelope, final BasicProperties properties, final byte[] body) throws IOException {
                Long scheduledDeliveryTime = getNumericHeader(properties, SCHEDULED_DELIVERY_HEADER);
                long delay;
                if (scheduledDeliveryTime != null) {
                    delay = scheduledDeliveryTime - System.currentTimeMillis();
                } else {
                    delay = 0;
                }
                
                Callable<Void> command = new Callable<Void>() {
                    @Override
                    public Void call() throws IOException {
                        channel.basicPublish(outboundQueueName, envelope.getRoutingKey(), properties, body);
                        return null;
                    }
                };
                
                executor.schedule(command, delay, TimeUnit.MILLISECONDS);
            }
        });
    }
    
    private static Long getNumericHeader(BasicProperties properties, String headerName) {
        Map<String, Object> headers = properties.getHeaders();
        if (headers != null) {
            Number headerValue = (Number) headers.get(headerName);
            if (headerValue != null) {
                return headerValue.longValue();
            }
        }
        return null;
    }
    
    @Override
    public void close() throws IOException {
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException("interrupted while waiting for executor to terminate", e);
        }
    }
    
}
