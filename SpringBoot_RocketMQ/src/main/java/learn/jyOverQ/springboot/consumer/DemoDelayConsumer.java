package learn.jyOverQ.springboot.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;

@Service
@RocketMQMessageListener(topic = "demo-delay-topic", consumerGroup = "demo_delay_consumer_group")
public class DemoDelayConsumer implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setInstanceName(consumer.getInstanceName() + "-"
                + ManagementFactory.getRuntimeMXBean().getName());
    }

    @Override
    public void onMessage(String message) {
        System.out.println("收到延时消息，时间=" + LocalDateTime.now() + "，消息=" + message);
    }
}
