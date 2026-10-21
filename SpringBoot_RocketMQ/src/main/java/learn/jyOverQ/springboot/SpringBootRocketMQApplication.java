package learn.jyOverQ.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// 只扫描当前包及子包；order、transaction 中的传统客户端演示单独运行。
@SpringBootApplication
public class SpringBootRocketMQApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootRocketMQApplication.class, args);
    }
}
