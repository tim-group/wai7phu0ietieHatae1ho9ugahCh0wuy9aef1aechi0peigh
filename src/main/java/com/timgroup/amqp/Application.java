package com.timgroup.amqp;

import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Application implements Closeable {
    
    public static void main(String... args) throws Exception {
        create(args[0]).start();
    }
    
    public static Application create(String configFilePath) throws Exception {
        Properties properties = loadProperties(configFilePath);
        
        String uri = properties.getProperty("uri");
        String inboundQueueName = properties.getProperty("inboundQueueName");
        String outboundQueueName = properties.getProperty("outboundQueueName");
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(uri);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        
        Transmitter transmitter = new Transmitter(channel, outboundQueueName);
        Receiver receiver = new Receiver(channel, inboundQueueName, transmitter);
        
        return new Application(connection, receiver, transmitter);
    }
    
    private static Properties loadProperties(String configFilePath) throws IOException {
        Properties properties = new Properties();
        FileReader configFileReader = new FileReader(configFilePath);
        properties.load(configFileReader);
        configFileReader.close();
        return properties;
    }
    
    private final Connection connection;
    private final Receiver receiver;
    private final Transmitter transmitter;
    
    public Application(Connection connection, Receiver receiver, Transmitter transmitter) {
        this.connection = connection;
        this.receiver = receiver;
        this.transmitter = transmitter;
    }
    
    public Receiver getReceiver() {
        return receiver;
    }
    
    public Transmitter getTransmitter() {
        return transmitter;
    }
    
    private void start() throws IOException {
        receiver.start();
    }
    
    @Override
    public void close() throws IOException {
        closeQuietly(receiver);
        closeQuietly(transmitter);
        connection.close();
    }
    
    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}
