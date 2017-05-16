package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.demo.FileChannelImp.AbstractFileChannel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProducer implements Producer {
    static AtomicInteger atomicInteger = new AtomicInteger(0);
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = null;
    //private FileManager fileManager = null;
    private KeyValue properties;
    int pos = 0;
    int proId;
    RandomAccessFile randomAccessFile = null;
    public DefaultProducer(KeyValue properties) {
        proId = atomicInteger.getAndIncrement();
        this.properties = properties;
        //fileManager = FileManager.FileManagerFactory(properties);
        messageStore = MessageStore.getInstance(properties.getString("STORE_PATH"));
        try {
            randomAccessFile = new RandomAccessFile(properties.getString("STORE_PATH")+"/producer"+proId,"wr");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public void send(Message message) {
        byte[] tmp = ((DefaultBytesMessage)message).getByteArray();
        try {
            randomAccessFile.write(tmp,pos,tmp.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        pos+=tmp.length;
    }

    @Override
    public void send(Message message, KeyValue properties) {
        DefaultBytesMessage bytesMessage = (DefaultBytesMessage) message;
        //bytesMessage.setProperties(properties);
        send(bytesMessage);
    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
}
