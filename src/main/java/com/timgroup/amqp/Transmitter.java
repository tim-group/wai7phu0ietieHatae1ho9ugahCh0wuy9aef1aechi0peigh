package com.timgroup.amqp;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

public class Transmitter implements Closeable {
    
    private final Channel channel;
    private final String queueName;
    private final ScheduledExecutorService executor;
    
    public Transmitter(Channel channel, String queueName) {
        this.channel = channel;
        this.queueName = queueName;
        this.executor = Executors.newSingleThreadScheduledExecutor();
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public void transmit(final String routingKey, final long deliveryTag, final BasicProperties properties, final byte[] body, long delay) {
        Callable<Void> command = new Callable<Void>() {
            @Override
            public Void call() throws IOException {
                channel.basicPublish(queueName, routingKey, properties, body);
                channel.basicAck(deliveryTag, false);
                return null;
            }
        };
        
        executor.schedule(command, delay, TimeUnit.MILLISECONDS);
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
