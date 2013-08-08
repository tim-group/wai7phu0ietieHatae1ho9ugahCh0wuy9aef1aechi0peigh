package com.timgroup.amqp;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.GetResponse;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ImmediateRepeatTest extends RepeatTestBase {
    
    @Test
    public void aMessageSentToTheInboundQueueIsRepeatedOnTheOutboundQueue() throws Exception {
        byte[] body = randomise("message").getBytes();
        testChannel.basicPublish(inboundQueueName, "", null, body);
        
        newTransceiver().start();
        
        GetResponse response = basicConsumeOnce(testChannel, outboundQueueName, 1, TimeUnit.SECONDS);
        assertArrayEquals(body, response.getBody());
    }
    
    @Test
    public void aRepeatedMessageHasItsOriginalMetadata() throws Exception {
        String routingKey = randomise("routing key");
        BasicProperties properties = randomiseProperties();
        testChannel.basicPublish(inboundQueueName, routingKey, properties, EMPTY_BODY);
        
        newTransceiver().start();
        
        GetResponse response = basicConsumeOnce(testChannel, outboundQueueName, 1, TimeUnit.SECONDS);
        assertEquals(routingKey, response.getEnvelope().getRoutingKey());
        assertPropertiesEquals(properties, response.getProps());
    }
    
    @Test
    public void aMessageWithoutAScheduledDeliveryHeaderIsRepeatedImmediately() throws Exception {
        newTransceiver().start();
        
        long expectedDeliveryTime = System.currentTimeMillis();
        testChannel.basicPublish(inboundQueueName, "", null, EMPTY_BODY);
        
        basicConsumeOnce(testChannel, outboundQueueName, 1, TimeUnit.SECONDS);
        long actualDeliveryTime = System.currentTimeMillis();
        
        assertDeliveredSoonAfter("the", expectedDeliveryTime, actualDeliveryTime);
    }
    
    @Test
    public void aMessageWithAScheduledDeliveryHeaderForATimeInThePastIsRepeatedImmediately() throws Exception {
        newTransceiver().start();
        
        long scheduledDeliveryTime = System.currentTimeMillis() - 1000;
        long expectedDeliveryTime = System.currentTimeMillis();
        BasicProperties propertiesWithScheduledDeliveryHeader = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime)).build();
        testChannel.basicPublish(inboundQueueName, "", propertiesWithScheduledDeliveryHeader, EMPTY_BODY);
        
        basicConsumeOnce(testChannel, outboundQueueName, 1, TimeUnit.SECONDS);
        long actualDeliveryTime = System.currentTimeMillis();
        
        assertDeliveredSoonAfter("the", expectedDeliveryTime, actualDeliveryTime);
    }
    
}
