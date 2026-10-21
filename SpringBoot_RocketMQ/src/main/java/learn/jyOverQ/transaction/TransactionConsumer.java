package learn.jyOverQ.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

// 普通消费者只能收到已经提交的事务消息，不能读取 Half Message。
public class TransactionConsumer {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tx_order_consumer_group");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("tx_order_topic", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                System.out.println("收到已提交的事务消息，orderId=" + message.getUserProperty("orderId")
                        + "，消息=" + new String(message.getBody(), StandardCharsets.UTF_8));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
        consumer.start();
        System.out.println("事务消息消费者已启动");
    }
}
