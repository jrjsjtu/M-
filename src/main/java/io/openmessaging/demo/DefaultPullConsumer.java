package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.*;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = null;
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private Map<String, Integer> topicOffsetMap = new HashMap<>();

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        messageStore = MessageStore.getInstance(properties.getString("STORE_PATH"));
    }


    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }

        Message message = null;
        //首先从Queue中获取
        try {
            message = messageStore.pullMessage(queue);
            if (message != null)
                return message;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //从Topic中获取
        int checkNum = 0;
        while (checkNum < bucketList.size()) {
            String bucket = bucketList.get(checkNum);
            int index = topicOffsetMap.getOrDefault(bucket, 0);
            message = messageStore.pullMessage(bucket, index);
            if (message != null) {
                topicOffsetMap.put(bucket, index + 1);
                return message;
            }
            checkNum++;
        }
        return null;
    }

    @Override
    public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);

    }


}
