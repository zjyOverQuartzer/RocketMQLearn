















# 高级特性：

## 1.消息存储：

### a.当 Broker 中的消息不存储到磁盘

> 如果Broker挂掉了，里面的数据就都丢失了,这种情况我们肯定不希望它发生

### b.把Broker 中的消息存储到数据库中，比如 MySQL

> 例如ActiveMQ 就是把消息存到数据库中
>
> 缺点：数据库会成为MQ的性能瓶颈，所以ActiveMQ 的处理速度比较慢

![image-20261016132134319](day4.assets/image-20261016132134319.png)

#### 为什么不在消费成功后立即删除消息？

> 因为 RocketMQ 支持多个消费者组独立消费同一个 Topic。
>
> 假设 Group A 消费完 5 条消息就直接将它们删除，那么 Group B 后面的 3 条消息就无法消费了。
>
> 当然消息一直没有被消费者消费，也可能因为超过存储保留期限而被删除。
>
> RocketMQ 不是永久存储系统。消费者长时间停止运行，就要注意消息保留时间和消费积压问题。

#### RocketMQ 的 Broker 接收到消息后，会把消息放在内存中，那么消费者消费成功后，Broker 内存中的消息什么时候被清理？

> 不会因为消费者消费成功就立即清理内存中的消息。RocketMQ 主要利用操作系统的 PageCache（页缓存）缓存消息，缓存什么时候被回收，主要由操作系统的内存管理机制决定。

#### Broker 存储在磁盘的消息什么时候会被删除呢？

假如 Broker 配置：

```properties
# 消息文件保留时间：72小时
fileReservedTime=72

# 每天凌晨4点执行定时清理检查
deleteWhen=04
```

>  假设某条消息在星期一上午 10 点写入 Broker，保留时间为 72 小时：
>
> + 星期一 10:01，消费者就已经成功消费。
> + 星期二，消息可能仍保留在磁盘。
> + 星期三，消息可能仍保留在磁盘。
> + 星期四 10:00，该消息达到 72 小时的保留时间。
>
> 但是，不代表星期四 10:00 这条消息就会立即被删除。
>
> RocketMQ 4.x 通常以 CommitLog 文件为单位清理过期数据，还要考虑清理检查时间、文件保留条件等。主要以 CommitLog 文件为单位删除，而不是每消费成功一条就删除一条。
>
> 另外，当磁盘使用率达到相应阈值时，也可能提前触发清理。

### c.把Broke 中的消息存储到本地文件

> 既然数据库也要存到本地磁盘，那我们可以直接存到本地磁盘。
>
> 采用消息刷盘机制进行数据存储。
>
> 这样性能更高，例如 RocketMQ、RabbitMQ、Kafka
>
> 缺点： 硬盘损坏的问题无法避免



## 2.高效的消息存储与读写方式：

### a. 高效写：

RocketMQ 采用的是 **CommitLog 顺序追加写** 的方式。在 RocketMQ 4.x 中，默认每个 CommitLog 文件大小为 **1 GiB（可配置）**。

RocketMQ 会创建固定大小的 CommitLog 文件，并将同一个 Broker 接收到的不同 Topic、不同 Queue 的完整消息，统一顺序追加到 CommitLog 文件末尾。

这种方式具有以下优势：

+ **减少随机磁盘 IO**：消息按照文件逻辑偏移量连续写入，减少随机寻址开销。
+ **提高写入吞吐量**：连续写入更有利于操作系统合并 IO 请求，充分利用磁盘的顺序写性能。
+ **减少文件扩容开销**：采用固定大小的文件分段，并可以提前创建后续文件，减少文件扩容和切换带来的开销。

> 需要注意的是，**顺序写指的是文件逻辑地址上的连续写入，并不意味着数据一定存储在连续的物理磁盘空间中**。实际物理存储位置由文件系统和存储设备管理。

![image-20261017084832662](day4.assets/image-20261017084832662.png)

为什么是1G?

> 可以自己设置 打开conf目录下的 我们用的两主两从模式，所以进入这个目录，打开对应Linux 配置文件 比如这个虚拟机用的是 broker-a.properties

![image-20261017085014461](day4.assets/image-20261017085014461.png)

打开之后可以修改对应的commitLog大小

<img src="day4.assets/image-20261017084930910.png" alt="image-20261017084930910" style="zoom:50%;" />

> RocketMQ 将不同 Topic、不同 Queue 的消息统一顺序追加到 CommitLog 文件中，避免了随机写带来的频繁磁盘寻址开销，提高了磁盘写入吞吐量。同时，通过 mmap 内存映射技术，将磁盘文件映射到进程的虚拟地址空间，利用操作系统的 PageCache 缓存数据。消息写入时，通常先写入 PageCache，再由操作系统和 RocketMQ 的刷盘机制将数据持久化到磁盘，减少直接等待磁盘 IO 的时间。在异步刷盘模式下，Broker 不必等待每条消息真正落盘后才返回发送结果，并且可以合并多次写入，进一步提升写入性能。



### b.高效读：

#### 1.先看一下Consumer是如何获取消息的

![image-20261017100128855](day4.assets/image-20261017100128855.png)

#### 2.ConsumeQueue：

如果没有 ConsumeQueue，就需要查找 CommitLog 中哪些消息属于这个队列，扫描成本高。

RocketMQ 通过 ConsumeQueue 建立索引：

| QueueOffset | CommitLogOffset | 消息大小 |
| ----------- | --------------- | -------- |
| 0           | 1000            | 200      |
| 1           | 1400            | 200      |
| 2           | 1800            | 200      |

这样消费者只需根据 `QueueOffset` 找到对应索引，然后通过 `CommitLogOffset` 读取完整消息。

RocketMQ 4.x 的 ConsumeQueue 索引条目采用固定长度 20 字节：

| 字段              | 大小    | 作用                      |
| ----------------- | ------- | ------------------------- |
| `CommitLogOffset` | 8 字节  | 消息在 CommitLog 中的位置 |
| `Size`            | 4 字节  | 消息长度                  |
| `TagsCode`        | 8 字节  | Tag 过滤相关编码          |
| 合计              | 20 字节 |                           |

因此，当队列中的 `QueueOffset=100` 时，对应索引条目在 ConsumeQueue 逻辑文件中的位置就是： 100*20=2000 字节

> 通过一个简单的乘法，就能定位索引条目的位置，再根据索引找到对应的消息位置。

#### 3.PageCache

> PageCache 是操作系统使用内存缓存文件数据的机制。

当 Broker 读取 CommitLog 中的消息时：

+ 如果消息所在的数据页已经在 PageCache 中，就可以直接从内存中的缓存访问。
+ 如果没有命中，就需要从磁盘加载对应的数据页，之后这些页也可以留在 PageCache 中。

> 这其实很类似于 Redis与MySQL等数据库的关系，核心机制都是把需要的数据放到更快的处理空间中，从而换得更快的响应速度。
>
> 当然Redis与MySQL这种关系是 借鉴的操作系统。

#### 4.mmap:减少文件访问开销

> Memory-Mapped File，内存映射文件

mmap 会将文件映射到进程的虚拟地址空间，使程序可以通过内存地址访问文件内容。

传统文件读取通常是：

```text
磁盘 → PageCache → 用户态缓冲区 → 业务处理
```

使用 mmap 后，在适用的读取路径中：

```text
磁盘 → PageCache（映射到进程虚拟地址空间）
                   ↓
               业务处理
```

这样可以减少常规 `read()` 路径中，从内核 PageCache 向用户态缓冲区复制数据的开销。

> 不过，mmap 不意味着消除了所有数据复制，也不意味着磁盘文件会完整加载到内存中。

#### 5.零拷贝：提高消息网络发送效率

> 消费者最终需要通过网络从 Broker 获取消息。

RocketMQ 4.x 在适用的消息传输路径上，可以使用 `FileRegion`、`transferTo` 等机制，利用操作系统的 `sendfile` 类能力，减少内核态和用户态之间不必要的数据复制。

如图是 传统IO与零拷贝的对比：

![传统IO与Linux sendfile路径对比](day4.assets/传统IO与Linux sendfile路径对比.png)

> 这个零拷贝是Linux操作系统的一种技术，也是一种思想，在Windows上也可以实现

> 在 Linux 上，RocketMQ 可以利用 Linux 的 `mmap`、`sendfile` 等机制。
>
> 在 Windows 上，也可以使用 Java NIO 和操作系统提供的相应能力。
>
> 对于生产环境，RocketMQ 通常更推荐部署在 Linux 上。

#### 6.批量拉取：减少网络交互次数

RocketMQ 的消费者通常不是每消费一条消息就单独向 Broker 发起一次网络请求，而是可以批量拉取多条消息。

#### 总结一下：

> RocketMQ 能够实现高效读取，核心原因是采用了 ConsumeQueue 索引 + mmap 内存映射 + PageCache 页缓存 + 零拷贝 + 批量拉取 等技术。
>
> 首先，RocketMQ 通过 ConsumeQueue 为每个 Topic 的 Queue 建立逻辑索引，消费者可以根据 QueueOffset 快速获取消息在 CommitLog 中的物理位置，避免扫描整个 CommitLog。其次，通过 mmap 内存映射和操作系统的 PageCache 缓存机制，热点消息可以直接从内存缓存中读取，减少磁盘 IO。同时，RocketMQ 在适用的网络传输路径中利用零拷贝技术，降低数据在内核态与用户态之间的复制开销。最后，通过批量拉取和长轮询，减少网络请求及调度开销，从而实现高吞吐、低延迟的消息读取。



## 3.消息存储结构：

如图是简单的消息存储结构：

![image-20261017161528293](day4.assets/image-20261017161528293.png)

> ConsumeQueue  记录消息在 CommitLog 中的物理位置，供消费者快速定位消息
>
> Consumer Offset  记录某个消费者组在某个队列上的消费进度
>
> IndexFile 当想查找某一条特定的历史消息时，可以通过消息的 Key 快速定位它，而不需要遍历整个 CommitLog。
>
> 
>
> 消息从生产者发送到 RocketMQ 中，由 CommitLog 存储完整信息， 
>
> > CommitLog 存储完整消息，ConsumeQueue 和 IndexFile 存储索引，并不是分别保存三份完整的消息。

这是课程里面的图：

![image-20261017161731975](day4.assets/image-20261017161731975.png)

###  RocketMQ 的三个存储结构：

| 存储结构       | 作用                    | 解决什么问题                        |
| -------------- | ----------------------- | ----------------------------------- |
| `CommitLog`    | 存储完整消息            | 消息保存在哪里？                    |
| `ConsumeQueue` | 维护队列的逻辑索引      | 消费者下一条应该读取哪条消息？      |
| `IndexFile`    | 维护消息 Key 的哈希索引 | 如何根据业务 Key 快速找到一条消息？ |

ConsumeQueue 适合按队列偏移量读取，IndexFile 适合按业务 Key 查询。

![image-20261017162911878](day4.assets/image-20261017162911878.png)

>  RocketMQ 4.x 中，ConsumeQueue 每个索引条目占 20 字节，默认每个索引文件可以容纳 30 万条索引：30 0000*20=600 0000字节  600 0000/1024<sup>2</sup> ≈5.72MB
>
> 所以显示的5.8M
>
> **5.8M 是索引文件的预设逻辑大小，不代表文件中已经写满了 30 万条消息索引。**

而真正的消费进度应该保存在类似这种目录下：

```cmd
store/
├── commitlog/
│   └── 完整消息数据
│
├── consumequeue/
│   └── TopicTest/
│       ├── 0/
│       ├── 1/
│       ├── 2/
│       └── 3/
│
├── index/
│   └── Key 索引文件
│
└── config/
    └── consumerOffset.json
        └── 消费者组的消费进度
```



#### a. CommitLog:

##### 1. 为什么需要 CommitLog？

假设有两个 Topic，每个 Topic 有不同的 Queue。

生产者发送以下消息：

```text
消息A → Topic1 / Queue0
消息B → Topic2 / Queue1
消息C → Topic1 / Queue1
消息D → Topic1 / Queue0
```

如果直接把完整消息分别写入不同的队列文件，就可能造成频繁的文件切换与分散写入。

RocketMQ 采用了更高效的设计：

同一个 Broker 接收的不同 Topic、Queue 的完整消息，统一顺序追加到 CommitLog。

特点：

+ 顺序追加写：提高磁盘写入吞吐量。

  > 消息按照文件逻辑偏移量连续写入，减少随机寻址开销。

+ 存储完整消息：包括消息体、Topic、QueueId、时间戳和各种属性。

+ 文件分段管理：RocketMQ 4.x 默认每个 CommitLog 文件大小为 1 GiB（可配置）。

  > 通过固定大小的文件分段和预创建机制，减少文件频繁创建、扩容和切换带来的开销。

+ 全局物理偏移量：通过 `commitLogOffset` 定位消息的存储位置。

##### 2. CommitLog 中每条消息存储什么？

| 字段             | 作用                        |
| ---------------- | --------------------------- |
| `TOTALSIZE`      | 整条消息的存储大小          |
| `MAGICCODE`      | 用于识别消息记录格式        |
| `BODYCRC`        | 消息体 CRC 校验             |
| `QUEUEID`        | 所属队列编号                |
| `QUEUEOFFSET`    | 在队列中的逻辑偏移量        |
| `PHYSICALOFFSET` | 在 CommitLog 中的物理偏移量 |
| `BORNTIMESTAMP`  | 生产者产生消息的时间        |
| `STORETIMESTAMP` | Broker 存储消息的时间       |
| `BODY`           | 真正的消息内容              |
| `TOPIC`          | 消息所属主题                |
| `PROPERTIES`     | Tag、Key 等扩展属性         |

> 这只是主要字段。RocketMQ 4.x 的完整消息记录还包含标志位、主机地址、重试次数、事务相关偏移量等。

####  b. ConsumeQueue：消息的逻辑队列索引

##### 1. 为什么需要 ConsumeQueue？

因为 CommitLog 中的消息是混合存储的。

例如现在有四个消息：

```text
CommitLog：
位置1000 → TopicA / Queue0 / 消息A
位置1200 → TopicB / Queue0 / 消息B
位置1400 → TopicA / Queue1 / 消息C
位置1600 → TopicA / Queue0 / 消息D
```

假设消费者只想读取： `TopicA / Queue0`, 需要找到消息 A 和消息 D。

如果每次都扫描整个 CommitLog，效率就很低。

因此 RocketMQ 为每个 Topic 的每个 Queue 建立独立的 ConsumeQueue 索引。

#####  2.ConsumeQueue 的数据结构

RocketMQ 4.x 的 ConsumeQueue 每条索引固定占用 20 字节。

![image-20261017164600388](day4.assets/image-20261017164600388.png)

例如 TopicA 的 Queue0：

| QueueOffset | CommitLogOffset | Size | TagsCode |
| ----------- | --------------- | ---- | -------- |
| 0           | 1000            | 200  | HashA    |
| 1           | 1600            | 200  | HashA    |

消费者想读取 QueueOffset=1 的消息，就可以直接根据固定长度计算索引位置：`1*20=20`

查到索引后，就知道要从 CommitLog 的位置 1600 读取 200 字节。

> 这就是 ConsumeQueue 高效的原因：通过逻辑偏移量快速定位索引，再利用物理偏移量找到完整消息，不需要扫描整个 CommitLog。
>
> 当然真正的消息内容仍然在 CommitLog 中。

### c. IndexFile：按 Key 查询消息

IndexFile 不是消费者正常顺序消费时必须走的索引，它主要用于消息查询。

例如生产者发送：

```java
Message msg = new Message(
    "OrderTopic",
    "TagA",
    "订单支付成功".getBytes(StandardCharsets.UTF_8)
);

msg.setKeys("ORDER_10001");
```

RocketMQ 可以为 `ORDER_10001` 建立查询索引。

概念上可以理解为：

```
IndexFile

ORDER_10001
     ↓
CommitLogOffset #某个消费者组消费到了哪个位置
     ↓
CommitLog 中的完整消息
```

实际 IndexFile 使用哈希槽和索引条目组织数据，查询时还需要处理哈希冲突等情况。

| 对比           | ConsumeQueue                | IndexFile              |
| -------------- | --------------------------- | ---------------------- |
| 主要用途       | 正常消息消费                | 根据 Key 查询消息      |
| 查找依据       | Topic、QueueId、QueueOffset | 消息 Key 等索引信息    |
| 存储内容       | 物理偏移量、长度、TagsCode  | Key 哈希、物理偏移量等 |
| 是否存完整消息 | 否                          | 否                     |

> 实际企业开发中，IndexFile 通常不是业务查询的主要手段。大部分业务数据查询会使用 MySQL，而 IndexFile 更多是用于 RocketMQ 消息排查、追踪和运维诊断。

## 4.刷盘机制：

同步刷盘：

![image-20261017170944600](day4.assets/image-20261017170944600.png)

异步刷盘：

![image-20261017171430299](day4.assets/image-20261017171430299.png)

如何选择：

![image-20261017171533215](day4.assets/image-20261017171533215.png)

如何配置：

![image-20261017171501301](day4.assets/image-20261017171501301.png)

![image-20261017171618892](day4.assets/image-20261017171618892.png)



## 5. 高可用性：

![image-20261017185948847](day4.assets/image-20261017185948847.png)

> **无状态是指：服务节点不依赖自身保存的持久化状态来提供服务，请求处理不依赖之前的请求历史。**
>
> 假设有三个 NameServer：
>
> 如果 NameServer 2 突然宕机：
>
> + NameServer 1 和 3 不受它的直接影响，可以继续提供路由查询。
> + Broker 仍然能够向其他正常的 NameServer 注册。
> + 客户端能够连接其他正常的 NameServer，就可以继续获取路由。
>
> 如果 NameServer 2 重启，它可以通过 Broker 后续重新注册，恢复路由信息，而不需要从其他 NameServer 复制持久化数据库。
>
> 
>
> **RocketMQ 的 NameServer 采用无状态设计，各节点相互独立，不需要彼此同步数据。Broker 会定期向所有配置的 NameServer 注册路由信息，NameServer 主要在内存中维护这些信息，不依赖路由数据的本地持久化。因此，单个 NameServer 宕机后，其他节点仍能提供服务；故障节点重启后，可以通过 Broker 重新注册恢复路由信息。**

### 主从复制的两种方式：

![image-20261017173524454](day4.assets/image-20261017173524454.png)

配置主从复制的方式：

![image-20261017190256484](day4.assets/image-20261017190256484.png)



## 6. 负载均衡：

### Producer 负载均衡： （写方面）

#### 一、生产者负载均衡策略

| 策略                | 核心原理                                      | 适用场景             |
| ------------------- | --------------------------------------------- | -------------------- |
| 轮询（Round Robin） | 按顺序依次选择不同的 MessageQueue             | 普通消息，默认策略   |
| 随机（Random）      | 随机选择一个可用 MessageQueue                 | 普通消息，自定义策略 |
| Hash 分配           | 根据订单 ID 等业务 Key 的 Hash 值选择固定队列 | 顺序消息、业务分区   |
| 自定义策略          | 根据业务规则选择指定队列                      | 机房优先、流量隔离等 |
| 故障规避            | 发送失败或延迟较高时，尽量避开异常 Broker     | 提高发送可用性       |

注意：这些并不是五种同级的内置默认算法。RocketMQ 4.x 默认采用轮询式选队列；随机、Hash 和其他特殊规则可通过 `MessageQueueSelector` 实现；故障规避则属于发送过程中的容错机制。

#### 二、轮询策略（默认）

假设 `TopicTest` 有 4 个可用队列，队列按 Q0 → Q1 → Q2 → Q3  →Q0 这样循环选择。

> 但这不代表每条消息大小相同，或每个 Broker 的实际负载一定完全相等。

####  三、Hash 分配策略（用于顺序消息）

例如一个订单会产生三条消息：

```
订单 1001：
    创建订单
    支付订单
    完成订单
```

如果使用默认轮询，可能出现：

```
创建订单 → Queue0
支付订单 → Queue1
完成订单 → Queue2
```

不同队列可以并行消费，因此可能出现消费顺序不一致的问题。

这时候可以根据订单 ID 选择队列：

示例：

```java
producer.send(msg, new MessageQueueSelector() {
    @Override
    public MessageQueue select(
            List<MessageQueue> mqs,
            Message msg,
            Object arg) {

        Long orderId = (Long) arg;

        int index = Math.floorMod(//保证索引非负
            orderId, mqs.size() //这里就是取模运算 只不过在被除数是负数是会有区别
        );
		//System.out.println(-10 % 3);              // -1
		//System.out.println(Math.floorMod(-10, 3)); // 2
        return mqs.get(index);
    }
}, orderId);
```

同一个订单 ID 会被映射到同一个队列（前提是可用队列列表及其顺序保持稳定）。

这样不同订单仍然可以分散到不同队列，提高并行处理能力。

> 不过，同一订单进入同一队列还不够：需要生产者按顺序发送、消费者使用顺序消费机制，才能正确保证消息顺序。

## 四、故障规避策略

假设 Topic 有两个 Broker：

```text
Broker A : Queue0、Queue1
Broker B : Queue0、Queue1
```

现在 Broker A 发生故障。

在发送重试时，RocketMQ 可以尽量选择其他 Broker 的队列。

RocketMQ 4.x 还提供发送延迟故障规避配置：

```java
producer.setSendLatencyFaultEnable(true);
```

开启后，客户端可以根据 Broker 最近的发送延迟和失败情况，在一定时间内规避异常 Broker。

> 它属于负载均衡基础上的容错优化

> RocketMQ 生产者的负载均衡主要由客户端完成。Producer 首先根据 Topic 获取可用 MessageQueue 的路由信息，然后选择目标队列发送消息。默认情况下，RocketMQ 采用轮询式策略，让消息相对均匀地分配到不同队列，从而分散 Broker 的写入压力。此外，开发者可以通过 `MessageQueueSelector` 实现随机选择、Hash 分配或其他业务路由策略。其中，Hash 分配常用于顺序消息，确保相同业务 Key 的消息进入同一队列。RocketMQ 还提供发送失败重试和延迟故障规避机制，尽量避免向异常 Broker 持续发送消息，提高发送可靠性。

### Consumer 负载均衡： （读方面）