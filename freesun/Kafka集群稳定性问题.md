#### Kafka集群稳定性问题

所有节点都指向1004这个连接报错。但是从每个节点上ping 1004所在服务器iota-m2是正常的。

```
[2025-05-28 21:58:51,623] WARN [ReplicaFetcherThread-0-1004], Error in fetch kafka.server.ReplicaFetcherThread$FetchRequest@5847f248 (kafka.server.ReplicaFetcherThread) java.io.IOException: Connection to 1004 was disconnected before the response was read
```

并排查m2上kafka服务：

```sh
sudo netstat -tnlp | grep :6667
# 或
sudo ss -tnlp | grep :6667
#防火墙状态
sudo ufw status

```



查看服务器性能指标：

```sh
top -c

htop

iostat -x 1

sudo bmon -p eno3

sudo iotop -aoP

pidstat -d 1 5

dstat -cdngy --top-io --top-bio
```



查看zookeeper状态，给每个节点依次发送：

```sh
echo ruok | nc <zk地址> 2181

```

返现Iota-m2有时反馈很慢。并且iota-m2在安装软件（apt）时也会出现卡顿。

另外从ZooKeeper 日志来看，**iota-m2 上的 ZooKeeper 确实存在连接不稳定问题**

导致 Kafka 集群中大量 `NotLeaderForPartitionException` 的最根本原因。

重新查看发现M2机器上WA较高：（25~30）

在 Linux 中，`wa`（即 `iowait`）代表 CPU 等待磁盘 I/O 完成的时间，占用高通常说明磁盘 IO 存在瓶颈，例如：

- 磁盘读写缓慢（机械硬盘尤甚）
- 某进程频繁读写大量数据（Kafka、ZooKeeper、日志等）
- RAID 或磁盘阵列故障
- 后台有大量 page cache flush 或 swap

 实用排查建议

| 工具         | 作用                         | 示例命令                          |
| ------------ | ---------------------------- | --------------------------------- |
| `iotop`      | 看哪个进程在做 I/O 等待      | `sudo iotop -oa`                  |
| `vmstat`     | 检查系统整体调度 & swap 情况 | `vmstat 1 5`                      |
| `pidstat -d` | 监控每个进程的 I/O           | `pidstat -d 1`                    |
| `top/htop`   | 看哪个进程占用高             | `top` / `htop`                    |
| `dstat`      | 综合查看 CPU/Mem/IO/Net      | `dstat -cdnm --top-io`            |
| `perf top`   | 内核热点函数实时分析         | `sudo perf top`（适用于高级分析） |



使用dstat工具查看：

```sh
root@iota-m2:/var/log/zookeeper# dstat -cdngy --top-io --top-bio
----total-cpu-usage---- -dsk/total- -net/total- ---paging-- ---system-- ----most-expensive---- ----most-expensive----
usr sys idl wai hiq siq| read  writ| recv  send|  in   out | int   csw |     i/o process      |  block i/o process
 10   1  88   0   0   1|  57k  875k|   0     0 |   0     0 |4010    22k|init       7182k 7106k|prometheus   45k  384k
  2   1  68  29   0   0|   0   136k| 732k  955k|   0     0 |7210    13k|dockerd    1996k  203k|java          0    84k
  2   1  73  24   0   0|   0     0 | 768k  897k|   0     0 |5957    11k|dockerd    1102k  356k|java          0    72k
  2   0  76  21   0   0|   0  4096B| 732k 1487k|   0     0 |5265  8897 |dockerd     479k  186k|java          0    80k
  2   1  76  21   0   0|   0     0 | 952k 1211k|   0     0 |6097    11k|dockerd     801k  317k|java          0    80k
  2   2  75  21   0   0|   0  8192B| 939k 1159k|   0     0 |6845    12k|dockerd     488k  186k|java          0    80k
  3   1  78  19   0   0|   0   120k| 780k  911k|   0     0 |6325    11k|dockerd    1160k  356k|java          0    76k
  2   1  75  22   0   0|   0     0 |1058k 1587k|   0     0 |6045    11k|python2.7   770k    0 |java          0    64k
  2   1  76  21   0   0|   0     0 |1059k 1595k|   0     0 |5858  9672 |dockerd     796k  287k|java          0    56k
  3   1  74  23   0   0|   0     0 |1722k 1757k|   0     0 |6220  9840 |dockerd     484k  216k|java          0    60k
  3   1  75  21   0   0|   0  2772k|1258k 2145k|   0     0 |8390    13k|prometheus 9778B 4097k|prometheus    0   828k
```

发现:

- 写入频率（如 `prometheus`, `java`, `jbd2/dm-0-8`）较高，有明显写入高峰。
- Prometheus 有一次达到 `read=4068k`、`write=2684k`，意味着在做大量数据抓取或压缩写入操作。



处理方法：将Prometheus服务从当前节点踢除。