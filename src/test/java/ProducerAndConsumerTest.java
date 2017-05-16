import io.openmessaging.Message;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.DefaultMessageFactory;
import io.openmessaging.tester.ConsumerTester;
import io.openmessaging.tester.ProducerTester;

import java.util.Properties;

public class ProducerAndConsumerTest {


    public static void main(String[] args) throws Exception {
        DefaultMessageFactory defaultMessageFactory = new DefaultMessageFactory();

        //ProducerTester.main(null);
        //ConsumerTester.main(null);
        Message message = defaultMessageFactory.createBytesMessageToTopic("test1","this is test".getBytes());
        message.putHeaders("intTest",1);
        message.putHeaders("StringTest","String");
        message.putHeaders("StringLong",1l);
        message.putHeaders("StringDouble",4.33);
        message.putProperties("StringTest","String");
        //message.putProperties("StringTest","String");
        byte[] tmp = ((DefaultBytesMessage)message).getByteArray();
        defaultMessageFactory.toMessage(tmp);
        int a = 0;
    }
}
