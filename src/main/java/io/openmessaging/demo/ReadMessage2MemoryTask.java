package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.util.concurrent.BlockingQueue;

/**
 * 将消息从磁盘读取到内存中的任务
 * Created by Xingfeng on 2017-05-15.
 */
public class ReadMessage2MemoryTask implements Runnable {

    private String parent;
    private String fileName;
    private BlockingQueue<Message> queue;

    private MessageDecoder messageDecoder = new EsayMessageDecoder();

    //k:v:t之间的分隔符
    private static final String ENTRY_SPLIT = " ";
    //k v t之间的分隔符
    private static final String K_V_T_SPLIT = ":";
    //换行符
    private static final String SEPARATOR = System.getProperty("line.separator");

    public ReadMessage2MemoryTask(String parent, String fileName, BlockingQueue<Message> queue) {
        this.parent = parent;
        this.fileName = fileName;
        this.queue = queue;
    }

    @Override
    public void run() {

        try {
            File file = new File(parent, fileName);
            if (file.exists()) {

                BufferedReader reader = null;
                FileInputStream in = null;
                try {
                    in = new FileInputStream(file);
                    reader = new BufferedReader(new InputStreamReader(in));

                    String line = null;
                    int len = 0;
                    DefaultBytesMessage defaultBytesMessage = null;
                    while ((line = reader.readLine()) != null) {


                        //headers的kv对
                        int headersNum = Integer.parseInt(line);
                        String headersLine = reader.readLine();
                        DefaultKeyValue headers = parseKVLine(headersLine);

                        //properties的kv对
                        int propertiesNum = Integer.parseInt(reader.readLine());
                        DefaultKeyValue properties = null;
                        //存在Properties
                        if (propertiesNum != 0) {
                            String propertiesLine = reader.readLine();
                            properties = parseKVLine(propertiesLine);
                        }

                        //body部分的长度
                        int bodyLength = Integer.parseInt(reader.readLine());
                        line = reader.readLine();
                        byte[] body = line.getBytes();

                        //组装消息
                        defaultBytesMessage = new DefaultBytesMessage(body);
                        defaultBytesMessage.setHeaders(headers);
                        if (properties != null) {
                            defaultBytesMessage.setProperties(properties);
                        }
                        queue.offer(defaultBytesMessage);
                    }
                } finally {

                    if (reader != null)
                        reader.close();

                    if (in != null) {
                        in.close();
                    }

                    //设置阻塞队列已满的状态
                    ((TopicLinkedBlockingQueue) queue).setStopPut(true);

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private DefaultKeyValue parseKVLine(String line) {

        DefaultKeyValue result = new DefaultKeyValue();
        String[] array = line.split(ENTRY_SPLIT);
        String[] kvts = null;
        for (String kvt : array) {

            kvts = kvt.split(K_V_T_SPLIT);
            String key = kvts[0];
            int type = Integer.parseInt(kvts[2]);
            switch (type) {
                case 0:
                    result.put(key, Integer.parseInt(kvts[1]));
                    break;
                case 1:
                    result.put(key, Long.parseLong(kvts[1]));
                    break;
                case 2:
                    result.put(key, Double.parseDouble(kvts[1]));
                    break;
                case 3:
                    result.put(key, kvts[1]);
                    break;
            }
        }
        return result;
    }

}
