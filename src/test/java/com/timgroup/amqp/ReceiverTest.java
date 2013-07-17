package com.timgroup.amqp;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.LongStringHelper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ReceiverTest extends IntegrationTest {
    
    private static final byte[] EMPTY_BODY = {};
    
    @Test
    public void aMessageSentToTheInboundQueueIsRepeatedOnTheOutboundQueue() throws Exception {
        byte[] body = randomise("message").getBytes();
        channel.basicPublish(inboundQueueName, "", null, body);
        
        new Receiver(channel, inboundQueueName, outboundQueueName).start();
        
        GetResponse response = basicConsumeOnce(channel, outboundQueueName, 1, TimeUnit.SECONDS);
        assertArrayEquals(body, response.getBody());
    }
    
    @Test
    public void aRepeatedMessageHasItsOriginalMetadata() throws Exception {
        String routingKey = randomise("routing key");
        String contentType = "application/" + randomise("x-content-type");
        String contentEncoding = randomise("content encoding");
        Map<String, Object> headers = singleHeader("header name", LongStringHelper.asLongString(randomise("header value"))); // header values come out as these weird strings
        int deliveryMode = 2;
        int priority = RANDOM.nextInt(7);
        String correlationId = randomise("correlation ID");
        String replyTo = randomise("reply to");
        String expiration = Integer.toString(9000 + RANDOM.nextInt(1000)); // is an integer in string's clothing
        String messageId = randomise("message ID");
        Date timestamp = new Date(1226534400000L + (RANDOM.nextInt(86400) * 1000)); // only preserved with second precision
        String type = randomise("type");
        String userId = TEST_BROKER_USERNAME; // needs to match, otherwise rejected
        String appId = randomise("app ID");
        String clusterId = randomise("cluster ID");
        
        BasicProperties.Builder propertiesBuilder = new BasicProperties.Builder().contentType(contentType)
                                                                                 .contentEncoding(contentEncoding)
                                                                                 .headers(headers)
                                                                                 .deliveryMode(deliveryMode)
                                                                                 .priority(priority)
                                                                                 .correlationId(correlationId)
                                                                                 .replyTo(replyTo)
                                                                                 .expiration(expiration)
                                                                                 .messageId(messageId)
                                                                                 .timestamp(timestamp)
                                                                                 .type(type)
                                                                                 .userId(userId)
                                                                                 .appId(appId)
                                                                                 .clusterId(clusterId);
        
        channel.basicPublish(inboundQueueName, routingKey, propertiesBuilder.build(), EMPTY_BODY);
        
        new Receiver(channel, inboundQueueName, outboundQueueName).start();
        
        GetResponse response = basicConsumeOnce(channel, outboundQueueName, 1, TimeUnit.SECONDS);
        assertEquals(routingKey, response.getEnvelope().getRoutingKey());
        BasicProperties receivedProperties = response.getProps();
        assertEquals(contentType, receivedProperties.getContentType());
        assertEquals(contentEncoding, receivedProperties.getContentEncoding());
        assertEquals(headers, receivedProperties.getHeaders());
        assertEquals((Object) deliveryMode, receivedProperties.getDeliveryMode());
        assertEquals((Object) priority, receivedProperties.getPriority());
        assertEquals(correlationId, receivedProperties.getCorrelationId());
        assertEquals(replyTo, receivedProperties.getReplyTo());
        assertEquals(expiration, receivedProperties.getExpiration());
        assertEquals(messageId, receivedProperties.getMessageId());
        assertEquals(timestamp, receivedProperties.getTimestamp());
        assertEquals(type, receivedProperties.getType());
        assertEquals(userId, receivedProperties.getUserId());
        assertEquals(userId, receivedProperties.getUserId());
        assertEquals(appId, receivedProperties.getAppId());
        assertEquals(clusterId, receivedProperties.getClusterId());
    }

    @Test
    public void aMessageWithoutAScheduledDeliveryHeaderIsRepeatedImmediately() throws Exception {
        new Receiver(channel, inboundQueueName, outboundQueueName).start();
        
        long expectedDeliveryTime = System.currentTimeMillis() + 100;
        channel.basicPublish(inboundQueueName, "", null, EMPTY_BODY);
        
        basicConsumeOnce(channel, outboundQueueName, 1, TimeUnit.SECONDS);
        long actualDeliveryTime = System.currentTimeMillis();
        
        // fractally awful assertion just to annoy Hamcrest fans
        if (actualDeliveryTime > expectedDeliveryTime) {
            assertEquals("message was repeated later than the expected delivery time", expectedDeliveryTime, actualDeliveryTime);
        }
    }
    
    @Test
    public void aMessageWithAScheduledDeliveryHeaderIsRepeatedAtTheAppointedTime() throws Exception {
        new Receiver(channel, inboundQueueName, outboundQueueName).start();
        
        long scheduledDeliveryTime = System.currentTimeMillis() + 1000;
        BasicProperties.Builder propertiesBuilder = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime));
        channel.basicPublish(inboundQueueName, "", propertiesBuilder.build(), EMPTY_BODY);
        
        basicConsumeOnce(channel, outboundQueueName, 2, TimeUnit.SECONDS);
        long actualDeliveryTime = System.currentTimeMillis();
        
        // fractally awful assertion just to annoy Hamcrest fans
        if (actualDeliveryTime < scheduledDeliveryTime) {
            assertEquals("message was repeated before scheduled delivery time", scheduledDeliveryTime, actualDeliveryTime);
        }
    }
    
    private Map<String, Object> singleHeader(String headerName, Object headerValue) {
        return Collections.singletonMap(headerName, headerValue);
    }
    
}
