package oneProducerToMutilConsumers;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

//单生产者
public class oneProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        //1. 首先我们要创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2. 应该给这个生产者绑定NameSever的地址
        producer.setNamesrvAddr("localhost:9876");
        //3. 启动生产者
        producer.start();
        //然后我们要发送多条消息
        String content = "Hello RocketMQ";
        for (int i = 0; i < 10; i++) {
            content=content+i;
            //给信息指定 topic  tag  内容
            Message msg= new Message("topic2", "tag1", content.getBytes());
            SendResult sendResult = producer.send(msg);
            //打印返回结果
            System.out.println("第"+i+"条返回结果" +sendResult);
        }
        //关闭连接
        producer.shutdown();
    }

}
