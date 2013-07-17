package com.timgroup.amqp;

import java.io.File;
import java.io.FileWriter;
import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ApplicationTest extends IntegrationTestBase {
    
    @Test
    public void applicationParsesConfigFileAndConnectsToBroker() throws Exception {
        File configFile = File.createTempFile("wai7", ".properties");
        configFile.deleteOnExit();
        Properties properties = new Properties();
        properties.setProperty("uri", TEST_BROKER_URI);
        properties.setProperty("inboundQueueName", inboundQueueName);
        properties.setProperty("outboundQueueName", outboundQueueName);
        properties.store(new FileWriter(configFile), null);
        
        Application application = Application.create(configFile.getPath());
        
        try {
            Receiver receiver = application.getReceiver();
            assertEquals(TEST_BROKER_HOST, receiver.getChannel().getConnection().getAddress().getHostName());
            assertEquals(inboundQueueName, receiver.getInboundQueueName());
            assertEquals(outboundQueueName, receiver.getOutboundQueueName());
        } finally {
            application.close();
        }
    }
    
}
