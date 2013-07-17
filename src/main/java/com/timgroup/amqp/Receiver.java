package com.timgroup.amqp;

import java.io.IOException;
import java.util.Map;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Receiver {
    
    public static final String SCHEDULED_DELIVERY_HEADER = "scheduled_delivery";
    
    private final Channel channel;
    private final String inboundQueueName;
    private final String outboundQueueName;
    
    public Receiver(Channel channel, String inboundQueueName, String outboundQueueName) {
        this.channel = channel;
        this.inboundQueueName = inboundQueueName;
        this.outboundQueueName = outboundQueueName;
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
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                Long scheduledDeliveryTime = getNumericHeader(properties, SCHEDULED_DELIVERY_HEADER);
                if (scheduledDeliveryTime != null) {
                    long delay = scheduledDeliveryTime - System.currentTimeMillis();
                    if (delay > 0) {
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            throw new IOException("interrupted before able to deliver", e);
                        }
                    }
                }
                
                channel.basicPublish(outboundQueueName, envelope.getRoutingKey(), properties, body);
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
    
}
