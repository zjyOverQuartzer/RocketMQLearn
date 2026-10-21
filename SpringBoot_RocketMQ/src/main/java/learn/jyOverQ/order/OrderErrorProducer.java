package learn.jyOverQ.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

// day3 第四节：默认多队列发送，不能保证同一订单跨队列有序。
public class OrderErrorProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_error_producer_group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 首次发送还需要发现路由、建立连接，演示中适当放宽超时。
        producer.setSendMsgTimeout(10000);
        producer.start();

        try {
            for (int i = 0; i < 10; i++) {
                String body = "订单1001，步骤：" + i;
                Message message = new Message(
                        "order_topic", "ORDER", body.getBytes(StandardCharsets.UTF_8));
                SendResult result = producer.send(message);
                System.out.printf("发送消息：%d，queueId=%d，queueOffset=%d%n",
                        i, result.getMessageQueue().getQueueId(), result.getQueueOffset());
            }
        } finally {
            producer.shutdown();
        }
    }
}
