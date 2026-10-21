package learn.jyOverQ.springboot.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;

@Profile("tag")
@Service
@RocketMQMessageListener(
        topic = "demo-topic",
        consumerGroup = "tag_consumer_group",
        selectorExpression = "tagA"
)
public class TagConsumer implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setInstanceName(consumer.getInstanceName() + "-"
                + ManagementFactory.getRuntimeMXBean().getName());
    }

    @Override
    public void onMessage(String message) {
        System.out.println("Tag 消费者收到 tagA 消息：" + message);
    }
}
