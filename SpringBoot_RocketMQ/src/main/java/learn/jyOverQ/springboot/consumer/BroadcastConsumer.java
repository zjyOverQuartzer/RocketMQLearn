package learn.jyOverQ.springboot.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;

@Profile("broadcast")
@Service
@RocketMQMessageListener(
        topic = "demo-topic",
        consumerGroup = "broadcast_consumer_group",
        messageModel = MessageModel.BROADCASTING
)
public class BroadcastConsumer implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setInstanceName(consumer.getInstanceName() + "-"
                + ManagementFactory.getRuntimeMXBean().getName());
    }

    @Override
    public void onMessage(String message) {
        System.out.println("广播消费者收到消息：" + message);
    }
}
