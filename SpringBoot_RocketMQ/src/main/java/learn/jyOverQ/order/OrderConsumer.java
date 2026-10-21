package learn.jyOverQ.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

// day3 5.3：固定队列发送还需要配合顺序监听器。
public class OrderConsumer {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer_group");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("order_topic", "*");
        consumer.registerMessageListener((MessageListenerOrderly) (messages, context) -> {
            try {
                for (MessageExt message : messages) {
                    String body = new String(message.getBody(), StandardCharsets.UTF_8);
                    System.out.println("顺序消费：" + body);
                }
                return ConsumeOrderlyStatus.SUCCESS;
            } catch (Exception e) {
                e.printStackTrace();
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
        consumer.start();
        System.out.println("顺序消费者已启动");
    }
}
