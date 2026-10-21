package learn.jyOverQ.transaction;

import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class TransactionProducerDemo {

    public static void main(String[] args) throws Exception {
        String scenario = args.length == 0 ? "commit" : args[0];
        if (args.length > 1 || !("commit".equals(scenario)
                || "rollback".equals(scenario) || "unknown".equals(scenario))) {
            throw new IllegalArgumentException("启动参数只能是 commit、rollback 或 unknown，默认 commit");
        }

        TransactionMQProducer producer = new TransactionMQProducer("tx_producer_group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setSendMsgTimeout(10000);
        producer.setTransactionListener(new DemoTransactionListener());
        Runtime.getRuntime().addShutdownHook(new Thread(producer::shutdown));
        producer.start();

        try {
            Message message = new Message(
                    "tx_order_topic", "订单创建事务消息".getBytes(StandardCharsets.UTF_8));
            message.putUserProperty("orderId", "ORDER_1001");

            TransactionSendResult result = producer.sendMessageInTransaction(message, scenario);
            System.out.println("事务消息发送调用已完成，orderId=ORDER_1001，场景="
                    + scenario + "，发送结果=" + result);
            System.out.println("保持生产者运行以响应事务回查，结束演示时请停止进程");
            new CountDownLatch(1).await();
        } finally {
            producer.shutdown();
        }
    }
}
