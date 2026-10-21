package learn.jyOverQ.springboot.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;

@Service
//添加一个监听器，指定订阅的Topic 消费者组consumerGroup
@RocketMQMessageListener(topic = "demo-topic", consumerGroup = "demo_consumer_group")
public class DemoConsumer implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        // Starter 2.0.3 的容器名在同机多进程间可能相同，追加 JVM 标识避免客户端冲突。
        consumer.setInstanceName(consumer.getInstanceName() + "-"
                + ManagementFactory.getRuntimeMXBean().getName());
    }

    @Override
    public void onMessage(String message) {
        System.out.println("收到 RocketMQ 消息：" + message);
        // 在这里编写真正的业务处理逻辑；处理失败时不要吞掉异常。
    }
}
