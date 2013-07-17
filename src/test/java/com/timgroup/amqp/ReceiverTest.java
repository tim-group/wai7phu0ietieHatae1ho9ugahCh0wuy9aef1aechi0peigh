package com.timgroup.amqp;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
        BasicProperties properties = randomiseProperties();
        channel.basicPublish(inboundQueueName, routingKey, properties, EMPTY_BODY);
        
        new Receiver(channel, inboundQueueName, outboundQueueName).start();
        
        GetResponse response = basicConsumeOnce(channel, outboundQueueName, 1, TimeUnit.SECONDS);
        assertEquals(routingKey, response.getEnvelope().getRoutingKey());
        assertPropertiesEquals(properties, response.getProps());
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
        BasicProperties propertiesWithScheduledDeliveryHeader = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime)).build();
        channel.basicPublish(inboundQueueName, "", propertiesWithScheduledDeliveryHeader, EMPTY_BODY);
        
        basicConsumeOnce(channel, outboundQueueName, 2, TimeUnit.SECONDS);
        long actualDeliveryTime = System.currentTimeMillis();
        
        // fractally awful assertion just to annoy Hamcrest fans
        if (actualDeliveryTime < scheduledDeliveryTime) {
            assertEquals("message was repeated before scheduled delivery time", scheduledDeliveryTime, actualDeliveryTime);
        }
    }
    
    @Test
    public void aMessageWithAScheduledDeliveryHeaderForATimeInThePastIsRepeatedImmediately() throws Exception {
        new Receiver(channel, inboundQueueName, outboundQueueName).start();
        
        long scheduledDeliveryTime = System.currentTimeMillis() - 1000;
        long expectedDeliveryTime = System.currentTimeMillis() + 100;
        BasicProperties propertiesWithScheduledDeliveryHeader = new BasicProperties.Builder().headers(singleHeader(Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime)).build();
        channel.basicPublish(inboundQueueName, "", propertiesWithScheduledDeliveryHeader, EMPTY_BODY);
        
        basicConsumeOnce(channel, outboundQueueName, 1, TimeUnit.SECONDS);
        long actualDeliveryTime = System.currentTimeMillis();
        
        // fractally awful assertion just to annoy Hamcrest fans
        if (actualDeliveryTime > expectedDeliveryTime) {
            assertEquals("message was repeated later than the expected delivery time", expectedDeliveryTime, actualDeliveryTime);
        }
    }
    
    @Test
    public void aScheduledMessageHasItsOriginalBodyAndMetadata() throws Exception {
        byte[] body = randomise("message").getBytes();
        String routingKey = randomise("routing key");
        BasicProperties properties = randomiseProperties();
        
        long scheduledDeliveryTime = System.currentTimeMillis() + 1000;
        BasicProperties propertiesWithScheduledDeliveryHeader = withHeader(properties, Receiver.SCHEDULED_DELIVERY_HEADER, scheduledDeliveryTime);
        channel.basicPublish(inboundQueueName, routingKey, propertiesWithScheduledDeliveryHeader, body);
        
        new Receiver(channel, inboundQueueName, outboundQueueName).start();
        
        GetResponse response = basicConsumeOnce(channel, outboundQueueName, 2, TimeUnit.SECONDS);
        assertArrayEquals(body, response.getBody());
        assertEquals(routingKey, response.getEnvelope().getRoutingKey());
        assertPropertiesEquals(propertiesWithScheduledDeliveryHeader, response.getProps());
    }
    
    private BasicProperties withHeader(BasicProperties properties, String headerName, Object headerValue) {
        Map<String, Object> headers = new HashMap<String, Object>(properties.getHeaders());
        headers.put(headerName, headerValue);
        return properties.builder().headers(headers).build();
    }
    
    private BasicProperties randomiseProperties() {
        BasicProperties.Builder propertiesBuilder = new BasicProperties.Builder().contentType("application/" + randomise("x-content-type"))
                                                                                 .contentEncoding(randomise("content encoding"))
                                                                                 .headers(singleHeader("header name", LongStringHelper.asLongString(randomise("header value")))) // header values come out as these weird strings
                                                                                 .deliveryMode(2)
                                                                                 .priority(RANDOM.nextInt(7))
                                                                                 .correlationId(randomise("correlation ID"))
                                                                                 .replyTo(randomise("reply to"))
                                                                                 .expiration(Integer.toString(9000 + RANDOM.nextInt(1000))) // is an integer in string's clothing
                                                                                 .messageId(randomise("message ID"))
                                                                                 .timestamp(new Date(1226534400000L + (RANDOM.nextInt(86400) * 1000))) // only preserved with second precision
                                                                                 .type(randomise("type"))
                                                                                 .userId(TEST_BROKER_USERNAME) // needs to match, otherwise rejected
                                                                                 .appId(randomise("app ID"))
                                                                                 .clusterId(randomise("cluster ID"));
        
        BasicProperties properties = propertiesBuilder.build();
        return properties;
    }
    
    private Map<String, Object> singleHeader(String headerName, Object headerValue) {
        return Collections.singletonMap(headerName, headerValue);
    }
    
    private void assertPropertiesEquals(BasicProperties expectedProperties, BasicProperties actualProperties) {
        assertEquals(expectedProperties.getContentType(), actualProperties.getContentType());
        assertEquals(expectedProperties.getContentEncoding(), actualProperties.getContentEncoding());
        assertEquals(expectedProperties.getHeaders(), actualProperties.getHeaders());
        assertEquals(expectedProperties.getDeliveryMode(), actualProperties.getDeliveryMode());
        assertEquals(expectedProperties.getPriority(), actualProperties.getPriority());
        assertEquals(expectedProperties.getCorrelationId(), actualProperties.getCorrelationId());
        assertEquals(expectedProperties.getReplyTo(), actualProperties.getReplyTo());
        assertEquals(expectedProperties.getExpiration(), actualProperties.getExpiration());
        assertEquals(expectedProperties.getMessageId(), actualProperties.getMessageId());
        assertEquals(expectedProperties.getTimestamp(), actualProperties.getTimestamp());
        assertEquals(expectedProperties.getType(), actualProperties.getType());
        assertEquals(expectedProperties.getUserId(), actualProperties.getUserId());
        assertEquals(expectedProperties.getAppId(), actualProperties.getAppId());
        assertEquals(expectedProperties.getClusterId(), actualProperties.getClusterId());
    }
    
}
