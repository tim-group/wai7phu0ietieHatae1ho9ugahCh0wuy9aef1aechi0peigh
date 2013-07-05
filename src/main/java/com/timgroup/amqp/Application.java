package com.timgroup.amqp;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Application {

    public void main(final Channel channel, String inboundQueueName, final String outboundQueueName) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        channel.basicConsume(inboundQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                channel.basicPublish(outboundQueueName, "", null, body);
            }
        });
    }
    
}
