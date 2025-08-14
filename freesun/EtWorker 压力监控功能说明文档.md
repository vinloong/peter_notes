# EtWorker 压力监控功能说明文档

## 1. 功能概述

压力监控功能是一个内置的背压控制和自动恢复机制，用于监控 Kafka 消费者的处理负载，防止系统过载，并在长时间高负载情况下提供自动通知和重启功能。

### 1.1 主要功能

- **背压控制**：当处理中的消息数量达到上限时，暂停新消息的拉取
- **压力监控**：实时跟踪系统压力状态和持续时间
- **邮件告警**：在压力持续一定时间后发送邮件通知
- **自动重启**：在压力持续过长时间后自动重启系统
- **压力解除通知**：压力缓解时发送恢复通知

### 1.2 工作原理

```
正常状态 → 压力状态 → 邮件通知 → 持续监控 → 自动重启
    ↑                                           ↓
    ← ← ← ← ← 压力解除 ← ← ← ← ← ← ← ← ← ← ← ← ← ←
```

## 2. 配置说明

### 2.1 基础配置

| 配置项           | 默认值 | 说明                           |
| ---------------- | ------ | ------------------------------ |
| `concurrent.max` | -1     | 最大并发处理数量，-1表示无限制 |

### 2.2 压力监控配置

| 配置项                          | 默认值              | 说明                           |
| ------------------------------- | ------------------- | ------------------------------ |
| `pressure.notify.threshold.ms`  | 300000 (5分钟)      | 压力持续多长时间后发送邮件通知 |
| `pressure.restart.threshold.ms` | 1800000 (30分钟)    | 压力持续多长时间后自动重启     |
| `pressure.notify.interval.ms`   | 600000 (10分钟)     | 重复邮件通知的间隔时           |
| `system.monitor.users`          | y**@free-sun.com.cn | 邮件通知人                     |

### 2.3 配置文件示例

```properties
# application.properties

# 基础并发控制
concurrent.max=1000

# 压力监控配置
# 5分钟后发送第一次通知
pressure.notify.threshold.ms=300000

# 30分钟后自动重启
pressure.restart.threshold.ms=1800000

# 每10分钟重复发送通知
pressure.notify.interval.ms=600000
```



## 3. 使用说明

### 3.1 基本使用

继承 `EtWorker` trait 的类会自动获得压力监控功能：

```scala
class KafkaWorker(topics: Set[String]) extends ShutdownableThread with EtWorker {
    
    override def init(props: Properties): Unit = {
        this.mprops = props
        // 其他初始化逻辑
    }
    
    override def doWork(): Unit = {
        // 在处理前检查压力状态
        if (!validate()) {
            logger.warn(s"Max concurrent handling reached: ${numHandling.get()}/$maxHandling, skipping poll")
            Thread.sleep(poll_duration_millis * 2)
            return
        }
        
        // 正常处理逻辑
        val records = consumer.poll(Duration.ofMillis(1000))
        if (records != null && !records.isEmpty) {
            handle(records)
        }
    }
    
    def handle(records: ConsumerRecords[String, String]): Unit = {
        val recordCount = records.count()
        
        // 增加处理计数
        addHandling(recordCount)
        
        try {
            // 处理消息逻辑
            processRecords(records)
        } finally {
            // 减少处理计数
            rmHandling(recordCount)
        }
    }
}
```

### 3.2 关键方法说明

#### validate()
```scala
def validate(): Boolean
```
- **功能**：检查当前是否可以处理更多消息
- **返回值**：true表示可以处理，false表示已达到上限
- **副作用**：自动更新压力状态，触发监控逻辑

#### addHandling(handlings: Long)

```scala
def addHandling(handlings: Long): Long
```
- **功能**：增加正在处理的消息计数
- **参数**：handlings - 要增加的处理数量
- **返回值**：增加前的计数值
- **使用时机**：开始处理消息时调用

#### rmHandling(handled: Long)

```scala
def rmHandling(handled: Long): Long
```
- **功能**：减少正在处理的消息计数
- **参数**：handled - 要减少的处理数量
- **返回值**：减少前的计数值
- **使用时机**：消息处理完成时调用

#### getPressureInfo()

```scala
def getPressureInfo(): Option[(Long, Long)]
```
- **功能**：获取当前压力状态信息
- **返回值**：Some((开始时间, 持续时间)) 或 None
- **用途**：用于外部监控和状态查询

### 3.3 邮件通知内容

#### 压力告警邮件

发送邮件给指定人员，提醒运维人员关注

```
主题：[ALERT] Kafka Consumer Under Pressure - WorkerClassName

内容：
Kafka Consumer has been under pressure for X minutes.

Current Status:
- Current Handling: 1000
- Max Handling: 1000
- Utilization: 100.0%
- Pressure Duration: 5 minutes

Worker: KafkaWorker
Time: 2025-01-15 10:30:00

Please check the system load and processing capacity.
```

#### 重启告警邮件

发送邮件给指定人员，并重启进程

```
主题：[CRITICAL] Kafka Consumer Restarting Due to Prolonged Pressure - WorkerClassName

内容：
Kafka Consumer has been under pressure for X minutes and will be restarted.

Final Status:
- Current Handling: 1000
- Max Handling: 1000
- Pressure Duration: 30 minutes

Worker: KafkaWorker
Time: 2025-01-15 11:00:00

System will restart automatically.
```

