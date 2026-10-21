# RocketMQLearn

RocketMQ 4.x 学习笔记与 Java 演示代码，记录从环境搭建、基础收发到 Spring Boot 整合、顺序消息、事务消息及集群原理的学习过程。

仓库按 `day1`～`day4` 整理教学文档，配合两个独立的 Maven 模块进行实验。适合按笔记逐步运行示例、观察消息发送和消费日志。

## 学习导航

| 文档 | 主要内容 |
| --- | --- |
| [day1：简单入手](docs/day1/day1简单入手.md) | MQ 基础概念、Windows 环境搭建、基础生产者和消费者、JDK 兼容问题 |
| [day2：消息收发与高级用法](docs/day2/day2.md) | 多消费者、多生产者、集群与广播消费、同步/异步/单向发送、延时、批量、Tag 和 SQL 过滤 |
| [day3：Spring Boot、顺序与事务消息](docs/day3/day3.md) | `RocketMQTemplate`、注解消费者、顺序消息对比、事务提交、回滚及回查 |
| [day4：集群与高级特性](docs/day4/day4.md) | 集群搭建、消息存储、刷盘、主从复制、负载均衡、重试、死信队列和幂等 |

部分示例以文档内的代码片段形式呈现，并非每一节都有独立 Java 类。实际可运行入口见下文。

## 主要目录

```text
RocketMQLearn/
├── docs/
│   ├── day1/                         # 基础概念、安装和简单收发
│   ├── day2/                         # 消费模式与消息发送方式
│   ├── day3/                         # Spring Boot、顺序消息、事务消息
│   └── day4/                         # 集群、存储和高级特性
├── rocketMQSimpleExample/            # 传统 Java Client 演示
│   ├── pom.xml
│   └── src/main/java/
│       ├── learn/jyOverQ/simple/
│       └── oneProducerToMutilConsumers/
├── SpringBoot_RocketMQ/              # Spring Boot 与 day3 演示
│   ├── pom.xml
│   └── src/main/
│       ├── java/learn/jyOverQ/
│       │   ├── springboot/           # 启动类、Controller、注解消费者
│       │   ├── order/                # 并发乱序与顺序消费对比
│       │   └── transaction/          # 事务消息及状态监听器
│       └── resources/application.properties
├── .gitignore
├── LICENSE
└── README.md
```

根目录没有聚合用的 `pom.xml`。两个模块分别构建、分别导入 IDEA，不存在 Maven 父子继承关系。

## 环境与版本

| 项目 | 当前配置 |
| --- | --- |
| Java | JDK 1.8 |
| Maven | 需安装 Maven，并确保 `mvn -version` 显示使用 JDK 1.8 |
| RocketMQ 服务端 | 按笔记使用 RocketMQ 4.8.0 |
| 基础模块依赖 | `rocketmq-client:4.8.0` |
| Spring Boot | `2.0.5.RELEASE` |
| RocketMQ Spring Starter | `2.0.3` |
| NameServer | 默认 `127.0.0.1:9876`，基础模块写作 `localhost:9876` |
| Spring Boot HTTP 端口 | 默认 `8080` |

两个模块独立管理依赖；Spring Boot 模块使用 Starter 传递引入的客户端。本仓库的示例属于 RocketMQ 4.x 传统 Java Client 体系。

## 快速开始

### 1. 获取代码并导入模块

```bash
git clone https://github.com/zjyOverQuartzer/RocketMQLearn.git
cd RocketMQLearn
```

在 IDEA 中打开仓库，分别将下面两个文件添加为 Maven 项目：

- [rocketMQSimpleExample/pom.xml](rocketMQSimpleExample/pom.xml)
- [SpringBoot_RocketMQ/pom.xml](SpringBoot_RocketMQ/pom.xml)

将项目 SDK、模块 SDK、Maven Runner 和运行配置的 JRE 设置为 **JDK 1.8**，等待 Maven 同步和索引完成。POM 中的 Java 8 编译设置不会自动把运行时 JDK 切换成 1.8。

### 2. 启动 NameServer 和 Broker

先根据 [day1 安装说明](docs/day1/day1简单入手.md) 安装 RocketMQ，并配置 `JAVA_HOME`、`ROCKETMQ_HOME`。

Windows 下，在 RocketMQ 解压目录的 `bin` 目录打开 **CMD**，依次执行：

```cmd
start mqnamesrv.cmd
start mqbroker.cmd -n 127.0.0.1:9876 autoCreateTopicEnable=true
```

等待 NameServer 和 Broker 都启动完成，并保持窗口运行。这里开启自动建 Topic 是为了本地学习；若你的环境未开启，请提前创建示例使用的 Topic。

NameServer 负责路由发现，实际收发还要求 Broker 正常运行，且它注册的地址和端口能被客户端访问。远程或虚拟机环境需要相应调整地址，不能只修改 NameServer 端口就认为服务已连通。

### 3. 构建

在仓库根目录打开终端，先确认 Java 和 Maven 使用的 JDK，再分别构建：

```powershell
java -version
mvn -version
mvn -f './rocketMQSimpleExample/pom.xml' clean package
mvn -f './SpringBoot_RocketMQ/pom.xml' clean package
```

构建不会代替消息收发验证。运行下面的示例时仍需保持 NameServer、Broker 可用，并结合发送结果和消费日志观察。

## 基础 Java Client 演示

在 `rocketMQSimpleExample` 模块中，先启动对应消费者，再运行生产者：

| 场景 | 消费者完整类名 | 生产者完整类名 | Topic |
| --- | --- | --- | --- |
| 简单收发 | `learn.jyOverQ.simple.Consumer` | `learn.jyOverQ.simple.Producer` | `Topic1` |
| 单生产者、多消费者 | `oneProducerToMutilConsumers.MutilConsumers` | `oneProducerToMutilConsumers.oneProducer` | `topic2` |

Topic 名称区分大小写，`Topic1` 与 `topic2` 是两个不同的 Topic。

`MutilConsumers` 当前显式使用 **广播消费**。要演示多个消费者，可以在 IDEA 中允许同一个运行配置同时启动多个实例，再运行生产者观察各实例日志。切换集群消费的代码位置在该类的 `setMessageModel()` 附近。

这两组示例都使用了 `group1` 消费组，但订阅的 Topic 不同。建议一次只运行其中一组；需要同时运行时，应为不同订阅配置独立的消费组，避免同组订阅不一致影响实验。

## Spring Boot 演示

### 启动应用

配置位于 [application.properties](SpringBoot_RocketMQ/src/main/resources/application.properties)：

```properties
server.port=8080
rocketmq.name-server=127.0.0.1:9876
rocketmq.producer.group=demo_producer_group
rocketmq.producer.send-message-timeout=3000
```

在 IDEA 中运行：

```text
learn.jyOverQ.springboot.SpringBootRocketMQApplication
```

也可以在构建后，从仓库根目录执行：

```powershell
java -jar './SpringBoot_RocketMQ/target/SpringBoot_RocketMQ-1.0-SNAPSHOT.jar'
```

访问 `http://localhost:8080/mq/send`，接口正常返回 `消息发送成功`；消费者正常接收到消息时，控制台打印 `收到 RocketMQ 消息：Hello RocketMQ`。

### HTTP 演示入口

下面的接口均为 GET 请求，以 `http://localhost:8080` 为地址前缀：

| 接口 | 演示内容 | Topic |
| --- | --- | --- |
| `/mq/send` | 同步发送 `Hello RocketMQ` | `demo-topic` |
| `/mq/async` | 异步发送，通过成功/失败回调观察结果 | `demo-topic` |
| `/mq/oneway` | 单向发送，不等待 Broker 返回发送结果 | `demo-topic` |
| `/mq/delay` | 延时消息，使用等级 2，常见默认配置对应 5 秒 | `demo-delay-topic` |
| `/mq/tag` | 发送带 `tagA` 的消息 | `demo-topic` |
| `/mq/sql?age=18` | 发送携带 `age` 用户属性的消息 | `demo-topic` |

接口返回、发送回调和消费者处理完成是不同阶段。异步接口返回只表示已发起调用；单向发送不提供常规的 Broker 成功确认。延时等级的实际时间以 Broker 配置为准。

### 选择消费者 Profile

默认启动基础消费者和延时消费者。以下消费者需通过 Spring Profile 显式启用：

| Profile | 消费者 | 行为 |
| --- | --- | --- |
| `tag` | `TagConsumer` | 只接收 `tagA` |
| `sql` | `SqlConsumer` | 使用 SQL92 条件 `age >= 18` |
| `broadcast` | `BroadcastConsumer` | 同组各实例分别接收广播消息 |

例如启用 Tag 过滤：

```powershell
java -jar './SpringBoot_RocketMQ/target/SpringBoot_RocketMQ-1.0-SNAPSHOT.jar' --spring.profiles.active=tag
```

IDEA 中把 `--spring.profiles.active=tag` 填入 **Program arguments（程序参数）** 即可。需要同时启用多个时使用 `--spring.profiles.active=tag,sql,broadcast`。

- **Tag：** 分别访问 `/mq/send`、`/mq/tag`，观察 `TagConsumer` 只接收后者。
- **SQL：** 先确认 Broker 的 `enablePropertyFilter=true` 已生效，再启用 `sql`；发送 `age=17` 和 `age=18` 作对比。
- **广播：** 启动两个带 `broadcast` Profile 的实例，为第二个增加 `--server.port=8081`，待两者启动完成后发送消息，观察两个实例各自的广播消费日志。

基础消费者与这些消费者使用不同消费组，因此它仍可能收到被 Tag/SQL 示例筛选掉的消息。广播实验中，基础消费者仍按自身消费组的集群模式分摊消息。

## 顺序消息与事务消息

这些类位于 `SpringBoot_RocketMQ` 模块，但采用独立的 Java `main()` 运行，不会随着 Spring Boot 应用自动执行。

| 场景 | 先启动消费者 | 再运行生产者 |
| --- | --- | --- |
| 并发乱序演示 | `learn.jyOverQ.order.OrderErrorConsumer` | `learn.jyOverQ.order.OrderErrorProducer` |
| 同订单顺序消费 | `learn.jyOverQ.order.OrderConsumer` | `learn.jyOverQ.order.OrderProducer` |
| 事务消息 | `learn.jyOverQ.transaction.TransactionConsumer` | `learn.jyOverQ.transaction.TransactionProducerDemo` |

### 顺序消息

两组顺序实验使用 `order_topic`。为观察队列分配和并发处理，准备多个读写队列，例如 4 个。

- 错误示例默认选择队列并使用并发监听器，连续发送步骤 0～9；消费日志可能乱序，但并不保证每次运行都出现乱序。
- 正确示例根据订单 ID 固定选择队列，并使用 `MessageListenerOrderly`。订单 1001、1002 各自按“创建 → 支付 → 完成”处理，不同订单之间可以交错。

一次运行当前场景对应的一对生产者和消费者。两组实验使用同一个 Topic，历史消息、消费位点以及其他生产者会影响日志观察。同业务键有序还要求上游按正确顺序发送，并考虑队列数量变化带来的路由变化。

### 事务消息

使用 Topic `tx_order_topic`。在 IDEA 中为 `TransactionProducerDemo` 设置下面的 **Program arguments**：

| 参数 | 演示行为 |
| --- | --- |
| 不填或 `commit` | 模拟本地事务成功，提交消息 |
| `rollback` | 模拟本地事务失败，回滚消息 |
| `unknown` | 第一次返回 `UNKNOW`，等待 Broker 回查后确认提交 |

事务生产者发送后会保持运行，以响应 Broker 回查，结束当前场景后再停止并切换参数。回查并非立即发生，具体等待时间取决于 Broker 配置。

`DemoTransactionListener` 使用内存 Map 模拟本地状态，消息中的业务标识为 `ORDER_1001`。各场景应依次演示，不要同时运行多个事务生产者。内存状态不能在进程重启后恢复，真实业务需要持久化事务记录，并设计下游重试和幂等。

## 常见问题

### 启动时报 `java.nio` 反射访问异常

基础模块的旧版 Netty 在较新 JDK 下可能出现 `InaccessibleObjectException` 或 `Could not initialize class ...PlatformDependent0`。本仓库按 JDK 1.8 运行，检查启动命令实际使用的 `java.exe`，不要只查看 POM 的编译版本。详细记录见 [day1](docs/day1/day1简单入手.md)。

### 连接、路由或发送超时

检查 NameServer、Broker 是否都已启动，NameServer 地址是否正确，Broker 注册的 IP/端口是否可访问，以及 Topic 是否存在。Spring Boot 默认发送超时为 3000 毫秒；顺序和事务演示中的三个独立生产者设置为 10000 毫秒，为首次路由发现和连接建立留出时间。

## License

本仓库采用 [MIT License](LICENSE)，版权信息见许可证文件。
