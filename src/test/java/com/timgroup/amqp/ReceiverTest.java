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
    public void aMessageSentToTheInboundQueueIsMovedToTheOutboundQueue() throws Exception {
        byte[] body = randomise("message").getBytes();
        channel.basicPublish(inboundQueueName, "", null, body);
        
        new Receiver(channel, inboundQueueName, outboundQueueName).start();
        
        GetResponse response = basicConsumeOnce(channel, outboundQueueName, 1, TimeUnit.SECONDS);
        assertArrayEquals(body, response.getBody());
    }
    
    @Test
    public void messageMetadataIsPreserved() throws Exception {
        String routingKey = "routing key";
        String contentType = "application/x-content-type";
        String contentEncoding = "content encoding";
        Map<String, Object> headers = Collections.singletonMap("header name", (Object) LongStringHelper.asLongString("header value")); // header values come out as these weird strings
        int deliveryMode = 2;
        int priority = 7;
        String correlationId = "correlation ID";
        String replyTo = "reply to";
        String expiration = "9001"; // is an integer in string's clothing
        String messageId = "message ID";
        Date timestamp = new Date(1226592681000L);
        String type = "type";
        String userId = TEST_BROKER_USERNAME; // needs to match, otherwise rejected
        String appId = "app ID";
        String clusterId = "cluster ID";
        
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
    
}
