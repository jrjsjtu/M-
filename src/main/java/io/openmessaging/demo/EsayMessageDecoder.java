package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Xingfeng on 2017-05-15.
 */
public class EsayMessageDecoder implements MessageDecoder {

    //k:v:t之间的分隔符
    private static final String ENTRY_SPLIT = " ";
    //换行符
    private static final String SEPARATOR = System.getProperty("line.separator");
    //换行符的字节数组
    private static final byte[] SEPARATOR_BYTES = SEPARATOR.getBytes();

    private List<String> linesList = new ArrayList<>();
    private int putIndex = 0, readIndex = 0;


    @Override
    public List<DefaultBytesMessage> lines2Message(String[] lines) {

        List<DefaultBytesMessage> messageList = new ArrayList<>();




        return messageList;
    }
}
