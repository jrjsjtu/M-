package io.openmessaging.demo.FileChannelImp;

import io.openmessaging.Message;

import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jrj on 17-5-16.
 */
public abstract class AbstractFileChannel {
    FileChannel fc;
    BlockingQueue<Byte[]> blockingQueue4byteArray;
    abstract void initFileChannel();
    abstract void send(Message message);
}
