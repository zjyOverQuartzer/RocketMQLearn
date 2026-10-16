package learn.jyOverQ.simple;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer {
    public static void main(String[] args) throws MQClientException {
        //1.谁来收？ 这里创建的是推送模式的Consumer也就是监听器模式  拉取模式对应的当然就是Pull
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.从哪里收？NameServer对应的地址和端口
        consumer.setNamesrvAddr("localhost:9876");
        //3.监听哪个Topic消息？ 通过下面函数指定监听哪个Topic 第二个参数是根据tag进行筛选
        //之后还有 根据bySQL进行筛选（可以根据属性值来筛选 比如 age>18的才会到达消费者这里）
        consumer.subscribe("Topic1","*");
        //4.  注册监听器 处理对应的业务流程
        //MessageListenerConcurrently() 这个监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //list 里面放的是MessageExt 也就是 生产者发送的消息
                //我们拿到对应的消息就可以进行处理对应的业务了
                for (MessageExt messageExt : list) {
                    System.out.println(messageExt);
                    byte[] body = messageExt.getBody();
                    System.out.println(new String(body));
                }
                //ConsumeConcurrentlyStatus 这个类是枚举类 之中只有两个属性
                //CONSUME_SUCCESS 接收成功  RECONSUME_LATER 稍后重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费者
        consumer.start();
        System.out.println("Consumer Started");

        //注意 这里不要关闭消费者的长连接，否则MQ中的消息无法推送过来了
    }
}
