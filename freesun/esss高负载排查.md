## ES高负载排查

ES中显示节点es-n3的负载是90。其他4个节点负载均正常。



|  负载/单核  |                           系统状态                           |
| :---------: | :----------------------------------------------------------: |
|  **< 0.7**  |                     非常轻松，资源充足。                     |
| **0.7-1.0** |             理想状态，资源利用率高但无显著排队。             |
| **1.0-1.5** |          轻微过载，短期峰值可能无害，但需关注趋势。          |
|  **> 1.5**  | 明显过载，任务排队严重，需立即排查（如高 CPU、磁盘 I/O 或死锁）。 |

查看ES角色：
GET `/_cat/nodes?v&h=name,node.role,master`

```sh
name  node.role master
es-n3 di        -
es-n2 di        -
es-n5 md        -
es-n1 md        *
es-n4 md        -
```



查看CPU：

```sh
nproc      # 直接显示总逻辑CPU数

grep -c "processor" /proc/cpuinfo
#32核心数

```



查看机器性能：

```sh
[root@cloud-es-n3 cloud]# top -c
top - 10:26:42 up 261 days, 14:55,  1 user,  load average: 67.99, 82.19, 94.32
Tasks: 345 total,   1 running, 344 sleeping,   0 stopped,   0 zombie
%Cpu(s):  9.5 us,  8.0 sy,  0.0 ni, 26.9 id, 53.9 wa,  0.0 hi,  1.7 si,  0.0 st
KiB Mem : 49224880 total,   295416 free, 41221672 used,  7707792 buff/cache
KiB Swap:        0 total,        0 free,        0 used.  7119016 avail Mem

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND
 8272 elastic+  20   0 3566.2g  43.2g   5.9g S 505.6 92.1  53757:59 /bin/java -Xms32512m -Xmx32512m -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -Des.n+
 6685 root      20   0  358200  23916   1808 S   0.0  0.0   0:12.37 /usr/bin/python -Es /usr/sbin/firewalld --nofork --nopid
```



- **`%id`（空闲）**：若 >50%，负载 1.4 可能是短暂峰值。
- **`%wa`（I/O 等待）**：若 >10%，负载高可能由磁盘瓶颈导致。

查看热线程：

```sh
GET /_nodes/hot_threads?type=cpu
{es-n3}{ghD6r-HgRnicYOtviOq1WA}{EzbLvQf1SzCejVf471Uekg}{10.8.40.12}{10.8.40.12:9300}{xpack.installed=true}
   Hot threads at 2025-04-21T02:31:23.604Z, interval=500ms, busiestThreads=3, ignoreIdleThreads=true:

   104.4% (522ms out of 500ms) cpu usage by thread 'elasticsearch[es-n3][search][T#99]'
     2/10 snapshots sharing following 42 elements
       org.apache.lucene.util.bkd.BKDReader.addAll(BKDReader.java:562)
       org.apache.lucene.util.bkd.BKDReader.addAll(BKDReader.java:562)
       org.apache.lucene.util.bkd.BKDReader.addAll(BKDReader.java:562)
       org.apache.lucene.util.bkd.BKDReader.addAll(BKDReader.java:562)
   103.7% (518.6ms out of 500ms) cpu usage by thread 'elasticsearch[es-n3][search][T#97]'
     6/10 snapshots sharing following 32 elements
       org.apache.lucene.search.Weight$DefaultBulkScorer.scoreAll(Weight.java:263)
       org.apache.lucene.search.Weight$DefaultBulkScorer.score(Weight.java:214)
       org.apache.lucene.search.BulkScorer.score(BulkScorer.java:39)
```

其中：

+ search 线程：高查询负载。
+ merge 线程：段合并压力大。
+ refresh 线程：索引刷新频繁。
+ bulk 线程：写入操作堆积。

> **节点角色**：`di`（data + ingest）
> 所以这个节点负责数据存储和预处理。
> 负载高达 **94.32**，持续升高
> 严重 CPU 和 IO 拥堵。
> 其他节点负载正常，说明负载不均或该节点被某些查询命中严重。
> **CPU 利用率情况**：
> us + sy 大约只有 17.5%，而 **wa（等待 IO）高达 53.9%**！
> 💡 说明该节点IO严重瓶颈（**磁盘瓶颈**，甚至可能是 SSD/NVMe 性能饱和）。
> ES 进程占用 92% 内存、500%+ CPU
> 单个 Java 进程消耗巨大，说明是在高强度做搜索操作。
> Hot threads 输出中是 BKDReader.addAll() 和 BulkScorer.score()
> 这是 Lucene 在处理大量 range 查询或 geo 查询、尤其是数值字段范围过滤时使用 BKD 树的表现。
> 即：某些复杂搜索导致 Lucene 拼命读取倒排索引或 BKD 索引，进而引发磁盘/CPU压力。



查询哪些语句导致查询慢：看看是否有请求耗时长、被卡在 es-n3 节点上。
GET _tasks?detailed=true&actions=*search

主要还是查询指定ID（设备或测点）+时间范围的数据查询。



检查磁盘：

```sh

[root@cloud-es-n3 cloud]# lsblk -d -o NAME,ROTA,HCTL,TYPE,SIZE,MODEL
NAME ROTA HCTL       TYPE  SIZE MODEL
sda     1 0:2:0:0    disk 10.5T PERC H310

iostat -x 1 60

```



问AI：

> 是否可能非磁盘性能问题，对比其他4个ES节点，本ES节点内存明显小于其他几个。它内存只有46G，其他几个为94G。ES设置的XMX为32G



#### ❗️为什么少内存可能会导致 IO 高？

Elasticsearch 是基于 Lucene 的搜索引擎，Lucene 的一大性能优化手段是**依赖 OS 的 page cache（操作系统页缓存）**。

当内存足够时：

- 文件会被缓存到内存中（page cache），减少磁盘访问。
- Elasticsearch 查询时大多是内存命中，IO 负载很轻。

而当内存不足时：

- 系统频繁触发 **page cache eviction（页缓存回收）**。
- 查询数据需要频繁从磁盘重新读取。
- **触发大量磁盘 IO 和等待（wa 高）**。
- 此时，哪怕磁盘性能再好，IO 密集度也大幅上升，容易形成瓶颈。



查询缓存命中：GET _nodes/stats/indices/fielddata,query_cache,request_cache

**1. 节点基础信息**

|         节点ID         | 节点名称 |   IP地址   | 端口 |     角色     | XPack安装 |
| :--------------------: | :------: | :--------: | :--: | :----------: | :-------: |
| ghD6r-HgRnicYOtviOq1WA |  es-n3   | 10.8.40.12 | 9300 | data, ingest |   true    |
| ZKP5NffCTNGvBPx6C3XspA |  es-n2   | 10.8.40.11 | 9300 | data, ingest |   true    |
| OKiN_ZZDRoimkuPjKHwW2A |  es-n5   | 10.8.40.14 | 9300 | master, data |   true    |
| cqYS88zIS02N4GXRYxpbbg |  es-n1   | 10.8.40.10 | 9300 | master, data |   true    |
| qsyU0WXkQNywTb_pZOigmQ |  es-n4   | 10.8.40.13 | 9300 | master, data |   true    |

------

**2. 查询缓存（Query Cache）**

| 节点名称 | 内存占用 (MB) |   总请求数    |    命中数     |   未命中数    | 缓存大小 |  缓存计数  |   淘汰数   |
| :------: | :-----------: | :-----------: | :-----------: | :-----------: | :------: | :--------: | :--------: |
|  es-n3   |    3163.8     | 3,383,304,324 | 1,532,559,391 | 1,850,744,933 |  46,755  | 35,971,426 | 35,924,671 |
|  es-n2   |    3116.7     | 2,641,624,804 | 1,408,702,925 | 1,232,921,879 |  63,728  | 22,323,783 | 22,260,055 |
|  es-n5   |    3167.5     | 2,324,352,271 | 1,264,290,880 | 1,060,061,391 |  67,149  | 24,342,587 | 24,275,438 |
|  es-n1   |    3169.6     | 3,355,553,135 | 1,915,481,398 | 1,440,071,737 |  68,893  | 27,985,124 | 27,916,231 |
|  es-n4   |    3140.0     | 2,716,695,745 | 1,411,630,272 | 1,305,065,473 |  77,943  | 21,447,518 | 21,369,575 |

------

**3. 字段数据（Fielddata）**

| 节点名称 | 内存占用 (KB) | 淘汰数 |
| :------: | :-----------: | :----: |
|  es-n3   |    1005.0     |   0    |
|  es-n2   |     628.2     |   0    |
|  es-n5   |     554.9     |   0    |
|  es-n1   |     655.1     |   0    |
|  es-n4   |     811.4     |   0    |

------

**4. 请求缓存（Request Cache）**

| 节点名称 | 内存占用 (MB) |   命中数   | 未命中数  | 淘汰数 |
| :------: | :-----------: | :--------: | :-------: | :----: |
|  es-n3   |     257.9     | 78,050,362 | 7,546,593 |   0    |
|  es-n2   |     300.0     | 88,048,473 | 3,036,814 |   0    |
|  es-n5   |     170.9     | 83,940,357 | 4,445,051 |   0    |
|  es-n1   |     311.7     | 83,213,420 | 5,355,418 | 1,582  |
|  es-n4   |     228.8     | 71,034,788 | 4,569,031 |   0    |

**设备IO统计**

| 节点名称 |    总操作数    |    读操作数    |  写操作数  | 读数据量 (TB) | 写数据量 (TB) |
| :------: | :------------: | :------------: | :--------: | :-----------: | :-----------: |
|  es-n3   | 26,704,202,709 | 26,616,942,962 | 87,259,747 |    131.21     |     2.14      |
|  es-n2   |   64,054,977   |   37,417,816   | 26,637,161 |     0.49      |     1.40      |
|  es-n5   |   62,678,711   |   41,979,843   | 20,698,868 |     0.45      |     0.54      |
|  es-n1   |  154,277,520   |  127,753,792   | 26,523,728 |     0.93      |     1.39      |
|  es-n4   |  106,961,873   |   81,679,391   | 25,282,482 |     0.62      |     1.07      |

 **es-n3 节点被严重“读放大”或“读压榨”**，差距是 **三个数量级（>400倍）** 的读操作差距

GET _cat/indices?v&s=docs.count:desc

#### 建议排查步骤



| 步骤         | 命令                                                 | 目的                          |                            |
| ------------ | ---------------------------------------------------- | ----------------------------- | -------------------------- |
| 🔍 分片分布   | `GET _cat/shards?v&h=index,shard,prirep,state,node`  | 查看是否集中在 es-n3          | 正常                       |
| 🔥 热索引分布 | `GET _cat/indices?v&s=docs.count:desc`               | 哪些大索引占比多              | 分片较均匀，未发现大分片或 |
| 🔄 缓存情况   | `GET _nodes/stats/indices/request_cache,query_cache` | 看 es-n3 缓存是否严重 miss    | MISS略高于其他             |
| 📊 索引写入量 | `GET _cat/indices?v&s=store.size:desc`               | 看是否是老数据重复读          |                            |
| 🧠 内存比较   | `GET _nodes/stats/jvm,os,fs`                         | 对比节点内存差异和 page cache | 较低：46<94G               |