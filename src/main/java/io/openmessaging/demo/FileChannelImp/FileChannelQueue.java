package io.openmessaging.demo.FileChannelImp;

import io.openmessaging.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * Created by jrj on 17-5-16.
 */
public class FileChannelQueue extends AbstractFileChannel {

    public FileChannelQueue(String path, String queue){
        try {
            //fc is defined in Abstractclass
            fc = new RandomAccessFile(path + "/" + queue, "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    void initFileChannel() {

    }

    @Override
    void send(Message message) {

    }
}
