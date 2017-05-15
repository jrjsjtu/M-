package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 负责管理Message的存放以及获取
 */
public class MessageStore {

    private static final int MAXIMUM_POOL_SIZE = 128;

    private static final ThreadFactory sThreadFactory = new ThreadFactory() {
        private final AtomicInteger mCount = new AtomicInteger(1);

        public Thread newThread(Runnable r) {
            return new Thread(r, "MessageStore#" + mCount.getAndIncrement());
        }
    };

    private static MessageStore INSTANCE = null;

    //消息存储目录
    private String parent;

    private ExecutorService executor;

    private MessageStore(String parent) {
        this.parent = parent;
        executor = Executors.newFixedThreadPool(MAXIMUM_POOL_SIZE, sThreadFactory);
    }

    public static MessageStore getInstance(String parent) {

        if (INSTANCE == null) {
            synchronized (MessageStore.class) {
                if (INSTANCE == null)
                    INSTANCE = new MessageStore(parent);
            }
        }

        return INSTANCE;
    }

    //关联topic或queue名与BlockingQueue
    private Map<String, BlockingQueue<Message>> messageBuckets = new HashMap<>();

    private Map<String, WriteMessage2DiskTask> writeTaskMap = new HashMap<>();

    public synchronized void putMessage(String bucket, Message message) {

        if (messageBuckets.get(bucket) == null) {
            LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>();
            messageBuckets.put(bucket, queue);
            WriteMessage2DiskTask writeMessage2DiskTask = new WriteMessage2DiskTask(parent, bucket, queue);
            writeTaskMap.put(bucket, writeMessage2DiskTask);
            executor.execute(writeMessage2DiskTask);
        }

        BlockingQueue<Message> queue = messageBuckets.get(bucket);
        queue.offer(message);
//        writeTaskMap.get(bucket).writeMessage(message);

    }

    //关联topic或queue与BlockingQueue
    private Map<String, TopicLinkedBlockingQueue<Message>> consumeMessageBuckets = new ConcurrentHashMap<>();

    /**
     * 从队列中获取Message
     *
     * @param queue
     * @return
     */
    public Message pullMessage(String queue) throws InterruptedException {

        TopicLinkedBlockingQueue<Message> queueBlockingQueue = consumeMessageBuckets.get(queue);
        if (queueBlockingQueue == null) {
            queueBlockingQueue = new TopicLinkedBlockingQueue<>();
            consumeMessageBuckets.put(queue, queueBlockingQueue);
            ReadMessage2MemoryTask task = new ReadMessage2MemoryTask(parent, queue, queueBlockingQueue);
            executor.execute(task);
        }

        while (!queueBlockingQueue.isStopPut()) {

            Message message = queueBlockingQueue.poll(5, TimeUnit.SECONDS);
            if (message != null)
                return message;

        }

        return null;
    }

    public Message pullMessage(String bucket, int index) {

        TopicLinkedBlockingQueue<Message> topicBlockingQueue = consumeMessageBuckets.get(bucket);
        if (topicBlockingQueue == null) {
            synchronized (this) {
                if (topicBlockingQueue == null) {
                    topicBlockingQueue = new TopicLinkedBlockingQueue<Message>();
                    consumeMessageBuckets.put(bucket, topicBlockingQueue);
                    ReadMessage2MemoryTask task = new ReadMessage2MemoryTask(parent, bucket, topicBlockingQueue);
                    executor.execute(task);
                }
            }
        }

        Message message = null;

        try {
            //已取完topic中的元素
            if (index >= topicBlockingQueue.size() && topicBlockingQueue.isStopPut())
                return null;

            message = topicBlockingQueue.take(index);

        } catch (InterruptedException e) {
            e.printStackTrace();
            message = null;
        }


        return message;
    }


}
