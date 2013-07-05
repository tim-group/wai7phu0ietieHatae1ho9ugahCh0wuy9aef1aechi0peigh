package com.timgroup.amqp;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Receiver {
    
    private final Channel channel;
    private final String inboundQueueName;
    private final String outboundQueueName;
    
    public Receiver(Channel channel, String inboundQueueName, String outboundQueueName) {
        this.channel = channel;
        this.inboundQueueName = inboundQueueName;
        this.outboundQueueName = outboundQueueName;
    }
    
    public void main() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        channel.basicConsume(inboundQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                channel.basicPublish(outboundQueueName, "", null, body);
            }
        });
    }
    
}
