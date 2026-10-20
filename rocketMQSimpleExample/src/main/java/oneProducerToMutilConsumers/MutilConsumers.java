package oneProducerToMutilConsumers;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class MutilConsumers {
    public static void main(String[] args) throws MQClientException {
        //1.创建一个接收消息的对象Consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.设定接收的命名服务器地址
        consumer.setNamesrvAddr("localhost:9876");
        //3.设置接收消息对应的topic,对应的sub标签为任意
        consumer.subscribe("topic2","*");
        //设置当前消费者的消费模式（默认模式：集群消费（负载均衡））
        //consumer.setMessageModel(MessageModel.CLUSTERING);
        //设置当前消费者的消费模式为广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //3.开启监听，用于接收消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //list 里面放的是MessageExt 也就是 生产者发送的消息
                //我们拿到对应的消息就可以进行处理对应的业务了
                for (MessageExt messageExt : list) {
                    System.out.println(messageExt);
                    byte[] body = messageExt.getBody();
                    //打印消息里面真正的内容：
                    System.out.println(new String(body));
                }
                //ConsumeConcurrentlyStatus 这个类是枚举类 之中只有两个属性
                //CONSUME_SUCCESS 接收成功  RECONSUME_LATER 稍后重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //4. 启动消费者
        consumer.start();
        System.out.println("Consumer Started");

        //注意 这里不要关闭消费者的长连接，否则MQ中的消息无法推送过来了
    }
}
