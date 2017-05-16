package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.demo.FileChannelImp.AbstractFileChannel;
import io.openmessaging.demo.FileChannelImp.FileChannelQueue;
import io.openmessaging.demo.FileChannelImp.FileChannelTopic;

import java.util.HashMap;

/**
 * Created by jrj on 17-5-16.
 */
public class FileManager {
    private static FileManager fileManager = null;
    public  static FileManager FileManagerFactory(KeyValue properties){
        if (fileManager == null){
            FileManager.properties = properties;
            fileManager = new FileManager();
        }
        return fileManager;
    }
    public static KeyValue properties;
    private HashMap<String,AbstractFileChannel> topicMap = new HashMap<>();
    private HashMap<String,AbstractFileChannel> QueueMap = new HashMap<>();
    public AbstractFileChannel validMap(String map){
        AbstractFileChannel tmp = topicMap.get(map);
        if (tmp == null){
            tmp = new FileChannelTopic(properties.getString("STORE_PATH"),map);
            topicMap.put(map,tmp);
        }
        return tmp;
    }

    public AbstractFileChannel validQueue(String queue){
        AbstractFileChannel tmp = QueueMap.get(queue);
        if (tmp == null){
            tmp = new FileChannelTopic(properties.getString("STORE_PATH"),queue);
            QueueMap.put(queue,tmp);
        }
        return tmp;
    }
}
