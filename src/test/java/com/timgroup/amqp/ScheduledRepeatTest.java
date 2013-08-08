package com.timgroup.amqp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.GetResponse;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ScheduledRepeatTest extends RepeatTestBase {
    
    @Test
    public void aMessageWithAScheduledDeliveryHeaderIsRepeatedAtTheAppointedTime() throws Exception {
        newTransceiver().start();
        
        long scheduledDeliveryTime = System.currentTimeMillis() + 1000;
        BasicProperties propertiesWithScheduledDeliveryHeader = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime)).build();
        testChannel.basicPublish(inboundQueueName, "", propertiesWithScheduledDeliveryHeader, EMPTY_BODY);
        
        basicConsumeOnce(testChannel, outboundQueueName, 2, TimeUnit.SECONDS);
        long actualDeliveryTime = System.currentTimeMillis();
        
        assertDeliveredSoonAfter("the", scheduledDeliveryTime, actualDeliveryTime);
    }
    
    @Test
    public void aScheduledMessageHasItsOriginalBodyAndMetadata() throws Exception {
        byte[] body = randomise("message").getBytes();
        String routingKey = randomise("routing key");
        BasicProperties properties = randomiseProperties();
        
        long scheduledDeliveryTime = System.currentTimeMillis() + 1000;
        BasicProperties propertiesWithScheduledDeliveryHeader = withHeader(properties, Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime);
        testChannel.basicPublish(inboundQueueName, routingKey, propertiesWithScheduledDeliveryHeader, body);
        
        newTransceiver().start();
        
        GetResponse response = basicConsumeOnce(testChannel, outboundQueueName, 2, TimeUnit.SECONDS);
        assertArrayEquals(body, response.getBody());
        assertEquals(routingKey, response.getEnvelope().getRoutingKey());
        assertPropertiesEquals(propertiesWithScheduledDeliveryHeader, response.getProps());
    }
    
    @Test
    public void aScheduledMessageDoesNotBlockFollowingMessages() throws Exception {
        newTransceiver().start();
        
        long farScheduledDeliveryTime = System.currentTimeMillis() + 1000;
        BasicProperties propertiesWithFarScheduledDeliveryHeader = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, farScheduledDeliveryTime)).build();
        byte[] firstMessageBody = randomise("message").getBytes();
        testChannel.basicPublish(inboundQueueName, "", propertiesWithFarScheduledDeliveryHeader, firstMessageBody);
        
        long nearScheduledDeliveryTime = System.currentTimeMillis() + 500;
        BasicProperties propertiesWithNearScheduledDeliveryHeader = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, nearScheduledDeliveryTime)).build();
        byte[] secondMessageBody = randomise("message").getBytes();
        testChannel.basicPublish(inboundQueueName, "", propertiesWithNearScheduledDeliveryHeader, secondMessageBody);
        
        long immediateDeliveryTime = System.currentTimeMillis();
        byte[] thirdMessageBody = randomise("message").getBytes();
        testChannel.basicPublish(inboundQueueName, "", null, thirdMessageBody);
        
        GetResponse firstResponse = basicConsumeOnce(testChannel, outboundQueueName, 1, TimeUnit.SECONDS);
        long firstActualDeliveryTime = System.currentTimeMillis();
        assertArrayEquals(thirdMessageBody, firstResponse.getBody());
        assertDeliveredSoonAfter("third", immediateDeliveryTime, firstActualDeliveryTime);
        
        GetResponse secondResponse = basicConsumeOnce(testChannel, outboundQueueName, 1, TimeUnit.SECONDS);
        long secondActualDeliveryTime = System.currentTimeMillis();
        assertArrayEquals(secondMessageBody, secondResponse.getBody());
        assertDeliveredSoonAfter("second", nearScheduledDeliveryTime, secondActualDeliveryTime);
        
        GetResponse thirdResponse = basicConsumeOnce(testChannel, outboundQueueName, 1, TimeUnit.SECONDS);
        long thirdActualDeliveryTime = System.currentTimeMillis();
        assertArrayEquals(firstMessageBody, thirdResponse.getBody());
        assertDeliveredSoonAfter("first", farScheduledDeliveryTime, thirdActualDeliveryTime);
    }
    
    @Test
    public void aScheduledMessageIsRemovedFromTheQueueOnceDelivered() throws Exception {
        newTransceiver().start();
        
        long scheduledDeliveryTime = System.currentTimeMillis() + 1000;
        BasicProperties propertiesWithScheduledDeliveryHeader = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime)).build();
        testChannel.basicPublish(inboundQueueName, "", propertiesWithScheduledDeliveryHeader, EMPTY_BODY);
        
        assertNotNull(basicConsumeOnce(testChannel, outboundQueueName, 2, TimeUnit.SECONDS));
        
        closeConnection(appConnection);
        
        // this assertion is a bit wacky, but there you go
        try {
            assertNull(basicConsumeOnce(testChannel, inboundQueueName, 1, TimeUnit.SECONDS));
        } catch (TimeoutException e) {}
    }
    
    /**
     * This test is about the fact that we don't acknowledge messages until
     * they're delivered. But you can't test that directly.
     */
    @Test
    public void aScheduledMessageIsLeftOnTheQueueIfItCannotBeDelivered() throws Exception {
        newTransceiver().start();
        
        long scheduledDeliveryTime = System.currentTimeMillis() + 1000;
        BasicProperties propertiesWithScheduledDeliveryHeader = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime)).build();
        testChannel.basicPublish(inboundQueueName, "", propertiesWithScheduledDeliveryHeader, EMPTY_BODY);
        
        Thread.sleep(100);
        closeConnection(appConnection);
        
        // these assertions are a bit wacky, but there you go
        assertNotNull(basicConsumeOnce(testChannel, inboundQueueName, 2, TimeUnit.SECONDS));
        try {
            assertNull(basicConsumeOnce(testChannel, outboundQueueName, 1, TimeUnit.SECONDS));
        } catch (TimeoutException e) {}
    }
    
    private BasicProperties withHeader(BasicProperties properties, String headerName, Object headerValue) {
        Map<String, Object> headers = new HashMap<String, Object>(properties.getHeaders());
        headers.put(headerName, headerValue);
        return properties.builder().headers(headers).build();
    }
    
}
