package learn.jyOverQ.simple;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

//一个简单的发送消息的生产者
public class Producer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        //1. 谁来发？ 新建一个默认的生产者 指定生产者组：
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2. 发给谁？ 发给本机的 NameServer 它会路由到对应的Broker
        producer.setNamesrvAddr("localhost:9876");
        producer.start(); //启动生产者
        //3. 怎么发？
        //4. 发什么？ 发消息 只不过要转成字节数组
        //因为网络传输通常都是以字节
        String msg="hello world1";
        Message message = new Message("Topic1", "tag1", msg.getBytes());
        SendResult sendResult = producer.send(message);
        //5. 发的结果是什么？
        System.out.println(sendResult);
        //6. 关闭链接
        producer.shutdown();
    }
}
