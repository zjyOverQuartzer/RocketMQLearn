package learn.jyOverQ.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

// day3 第五节：同一订单固定路由到同一个队列。
public class OrderProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_producer_group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setSendMsgTimeout(10000);
        producer.start();

        try {
            sendOrderEvent(producer, 1001L, "创建");
            sendOrderEvent(producer, 1002L, "创建");
            sendOrderEvent(producer, 1001L, "支付");
            sendOrderEvent(producer, 1002L, "支付");
            sendOrderEvent(producer, 1001L, "完成");
            sendOrderEvent(producer, 1002L, "完成");
        } finally {
            producer.shutdown();
        }
    }

    private static void sendOrderEvent(DefaultMQProducer producer, long orderId, String event)
            throws Exception {
        String body = "订单号：" + orderId + "，事件：" + event;
        Message message = new Message(
                "order_topic", "ORDER", body.getBytes(StandardCharsets.UTF_8));

        producer.send(message, (queues, msg, arg) -> {
            long id = (Long) arg;
            // Java 8 的 floorMod(long, long) 返回 long，取模后再转换为队列索引。
            int index = (int) Math.floorMod(id, (long) queues.size());
            return queues.get(index);
        }, orderId);
    }
}
