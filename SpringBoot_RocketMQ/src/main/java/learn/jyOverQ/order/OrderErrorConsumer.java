package learn.jyOverQ.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

// day3 第四节：并发消费可能乱序，日志不一定每次都出现乱序。
public class OrderErrorConsumer {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_error_consumer_group");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("order_topic", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                System.out.printf("线程=%s，queueId=%d，queueOffset=%d，消息=%s%n",
                        Thread.currentThread().getName(), message.getQueueId(),
                        message.getQueueOffset(), body);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
        consumer.start();
        System.out.println("并发消费者已启动，请运行 OrderErrorProducer 观察日志");
    }
}
