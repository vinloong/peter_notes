### ET假消费问题排查

安心云多实例分布式ET出现部分实例只有Kafka消费记录，无任何数据处理（Actor处理）日志。

可能是数据量过大/或性能（内存）瓶颈限制导致。

本次优化内容：

1. 缓存库切换（Guava->caffeine ）

   > 之前出现过缓存超时日志，提升缓存性能

2. 增加kafka背压机制以及压力监控

   > 长时间运行数据压力监测。邮件通知并达到一定压力位后重启服务。
   >
   > 配置方法参考《EtWorker 压力监控功能说明文档》



排查主要从日志出发，当出现假消费时候，日志一直在重复，主要分为以下三类

```txt
# kafka消费日志
2025-08-10 00:03:28,887 INFO  singleton$:227 % -  actor 发送 capId=[48082fc2-e551-42b2-b764-958276fade93.74b8f2d2-b58c-4c7f-9af1-71fdc9aa3b08],len= 3
2025-08-10 00:03:28,990 INFO  singleton$:210 % - dataWork =>  3 条
2025-08-10 00:03:28,990 INFO  singleton$:227 % -  actor 发送 capId=[48082fc2-e551-42b2-b764-958276fade93.74b8f2d2-b58c-4c7f-9af1-71fdc9aa3b08],len= 3
2025-08-10 00:03:29,093 INFO  singleton$:210 % - dataWork =>  1 条

...
# 微功耗离线忽略记录
2025-08-10 00:03:41,189 INFO  RecvDataHanler$:52 % - class et.recv.RecvDataHanler$micro-power device unlink alarm ignore.4476a742-491e-4e54-ad80-47cae9300cf3 无线倾角25302214111
2025-08-10 00:03:41,190 INFO  RecvDataHanler$:52 % - class et.recv.RecvDataHanler$micro-power device unlink alarm ignore.f03aa758-aeda-4ec0-9d03-8f962ec89856 无线倾角25302218111

...
# 偶发：Guava缓存超时问题
java.util.concurrent.ExecutionException: java.util.concurrent.TimeoutException: Futures timed out after [10 seconds]
	at com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:299)
	at com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:286)
	at com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116)
	at com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly(Uninterruptibles.java:135)
	at com.google.common.cache.LocalCache$Segment.getAndRecordStats(LocalCache.java:2346)
	at com.google.common.cache.LocalCache$Segment.loadSync(LocalCache.java:2318)
	at com.google.common.cache.LocalCache$Segment.lockedGetOrLoad(LocalCache.java:2280)
	at com.google.common.cache.LocalCache$Segment.get(LocalCache.java:2195)
	at com.google.common.cache.LocalCache.get(LocalCache.java:3934)
	at com.google.common.cache.LocalCache.getOrLoad(LocalCache.java:3938)
	at com.google.common.cache.LocalCache$LocalLoadingCache.get(LocalCache.java:4821)
	at comm.utils.ConfigHelper$.getDeviceInfo(ConfigHelper.scala:237)
	at et.recv.RecvDataHanler$.onAlarmHandler(RecvDataHanler.scala:530)
	at et.main.singleton$$anonfun$alarmWork$1.apply(singleton.scala:319)
	at et.main.singleton$$anonfun$alarmWork$1.apply(singleton.scala:318)
	at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
	at et.main.singleton$.alarmWork(singleton.scala:318)
	at et.main.singleton$$anonfun$main$16.apply(singleton.scala:180)
	at et.main.singleton$$anonfun$main$16.apply(singleton.scala:180)
	at comm.utils.KafkaWorker$$anonfun$doWork$2.apply(KafkaWorker.scala:234)
	at comm.utils.KafkaWorker$$anonfun$doWork$2.apply(KafkaWorker.scala:212)
	at scala.collection.immutable.Map$Map2.foreach(Map.scala:137)
	at comm.utils.KafkaWorker.doWork(KafkaWorker.scala:212)
	at kafka.utils.ShutdownableThread.run(ShutdownableThread.scala:82)
Caused by: java.util.concurrent.TimeoutException: Futures timed out after [10 seconds]
```



后续排查建议：

1. 持续跟踪ET内存消耗，判断是否内存瓶颈导致消费积压
2. 提高Kafka分片数后增加ET消费者实例。