package com.timgroup.amqp;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.rabbitmq.client.GetResponse;

import static org.junit.Assert.assertArrayEquals;

public class ReceiverTest extends IntegrationTest {
    
    @Test
    public void aMessageSentToTheInboundQueueIsMovedToTheOutboundQueue() throws Exception {
        byte[] body = randomise("message").getBytes();
        channel.basicPublish(inboundQueueName, "", null, body);
        
        new Receiver().main(channel, inboundQueueName, outboundQueueName);
        
        GetResponse response = basicConsumeOnce(channel, outboundQueueName, 1, TimeUnit.SECONDS);
        assertArrayEquals(body, response.getBody());
    }
    
}
