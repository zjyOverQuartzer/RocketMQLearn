package learn.jyOverQ.springboot.controller;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/mq")
public class DemoProducerController {

    private final RocketMQTemplate rocketMQTemplate;

    public DemoProducerController(RocketMQTemplate rocketMQTemplate) {
        this.rocketMQTemplate = rocketMQTemplate;
    }

    // 同步发送，不等待消费者完成业务处理。
    @GetMapping("/send")
    public String sendMessage() {
        // 向 demo-topic 发送消息
        rocketMQTemplate.convertAndSend("demo-topic", "Hello RocketMQ");
        return "消息发送成功";
    }

    // 发送结果在回调中处理，接口返回不代表发送已成功。
    @GetMapping("/async")
    public String sendAsyncMessage() {
        rocketMQTemplate.asyncSend("demo-topic", "异步发送消息", new SendCallback() {
            @Override
            public void onSuccess(SendResult result) {
                System.out.println("发送成功：" + result);
            }

            @Override
            public void onException(Throwable e) {
                System.err.println("发送失败：" + e.getMessage());
                e.printStackTrace();
            }
        });
        return "已发起异步发送，请查看回调日志";
    }

    // day3 3.2：单向发送不等待 Broker 返回发送结果。
    @GetMapping("/oneway")
    public String sendOneWayMessage() {
        rocketMQTemplate.sendOneWay("demo-topic", "单向发送消息");
        return "单向发送调用已完成，不代表已获得 Broker 的成功确认";
    }

    // day3 3.3：2 是延时等级，默认对应 5 秒，实际以 Broker 配置为准。
    @GetMapping("/delay")
    public String sendDelayMessage() {
        SendResult result = rocketMQTemplate.syncSend(
                "demo-delay-topic",
                MessageBuilder.withPayload("延时消息").build(),
                3000,
                2
        );
        return "延时消息发送结果：" + result;
    }

    // day3 3.4：destination 中的冒号用于分隔 Topic 和 Tag。
    @GetMapping("/tag")
    public String sendTagMessage() {
        rocketMQTemplate.convertAndSend("demo-topic:tagA", "带有 tagA 的消息");
        return "tagA 消息发送调用已完成";
    }

    // day3 3.5：Starter 将 age Header 转换为 RocketMQ 用户属性。
    @GetMapping("/sql")
    public String sendSqlMessage(@RequestParam(defaultValue = "18") int age) {
        rocketMQTemplate.syncSend(
                "demo-topic",
                MessageBuilder.withPayload("SQL 属性过滤消息，age=" + age)
                        .setHeader("age", age)
                        .build()
        );
        return "SQL 过滤演示消息发送调用已完成，age=" + age;
    }
}
