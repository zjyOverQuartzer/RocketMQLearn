package learn.jyOverQ.springboot.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;

// 先确认 Broker 启用了属性过滤，再通过 sql Profile 启用此消费者。
@Profile("sql")
@Service
@RocketMQMessageListener(
        topic = "demo-topic",
        consumerGroup = "sql_consumer_group",
        selectorType = SelectorType.SQL92,
        selectorExpression = "age >= 18"
)
public class SqlConsumer implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setInstanceName(consumer.getInstanceName() + "-"
                + ManagementFactory.getRuntimeMXBean().getName());
    }

    @Override
    public void onMessage(String message) {
        System.out.println("SQL 消费者收到 age >= 18 的消息：" + message);
    }
}
