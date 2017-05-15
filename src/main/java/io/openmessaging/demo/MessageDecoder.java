package io.openmessaging.demo;

import java.util.List;

/**
 * Created by Xingfeng on 2017-05-15.
 */
public interface MessageDecoder {

    List<DefaultBytesMessage> lines2Message(String[] lines);

}
