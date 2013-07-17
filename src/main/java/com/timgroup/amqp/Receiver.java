package com.timgroup.amqp;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Receiver implements Closeable {
    
    public static final String SCHEDULED_DELIVERY_HEADER = "scheduled_delivery";
    
    private final Channel channel;
    private final String queueName;
    private final Transmitter transmitter;
    private String consumerTag;
    
    public Receiver(Channel channel, String queueName, Transmitter transmitter) {
        this.channel = channel;
        this.queueName = queueName;
        this.transmitter = transmitter;
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public void start() throws IOException {
        consumerTag = channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                Long scheduledDeliveryTime = getNumericHeader(properties, SCHEDULED_DELIVERY_HEADER);
                long delay = scheduledDeliveryTime != null ? scheduledDeliveryTime - System.currentTimeMillis() : 0;
                
                transmitter.transmit(envelope.getRoutingKey(), properties, body, delay);
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
        if (consumerTag != null) {
            channel.basicCancel(consumerTag);
            consumerTag = null;
        }
    }
    
}
