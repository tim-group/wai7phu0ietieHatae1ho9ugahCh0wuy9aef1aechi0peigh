package com.timgroup.amqp;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.impl.LongStringHelper;

import static org.junit.Assert.assertEquals;

public abstract class RepeatTestBase extends IntegrationTestBase {
    
    protected static final byte[] EMPTY_BODY = {};
    
    protected void assertDeliveredSoonAfter(String message, long expectedDeliveryTime, long actualDeliveryTime) {
        if (actualDeliveryTime < expectedDeliveryTime) {
            assertEquals(message + " message was repeated before scheduled delivery time", expectedDeliveryTime, actualDeliveryTime);
        }
        if (actualDeliveryTime > expectedDeliveryTime + 100) {
            assertEquals(message + " message was repeated later than the scheduled delivery time", expectedDeliveryTime, actualDeliveryTime);
        }
    }
    
    protected BasicProperties randomiseProperties() {
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
    
    protected Map<String, Object> singleHeader(String headerName, Object headerValue) {
        return Collections.singletonMap(headerName, headerValue);
    }
    
    protected void assertPropertiesEquals(BasicProperties expectedProperties, BasicProperties actualProperties) {
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
