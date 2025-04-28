## DAC相关知识分享

> 物联网平台的核心组件，负责设备通信和数据采集。



#### :cloud: 环境部署

![image-20250428133838359](C:\Users\yww08\AppData\Roaming\Typora\typora-user-images\image-20250428133838359.png)

这里几个进程的命名：

| 名称         | 说明                                                    |      |
| ------------ | ------------------------------------------------------- | ---- |
| DAC          | **D**ata **A**cquisition **C**ontroller  本次分享的主角 |      |
| Proxy        | 接入网关代理。                                          |      |
| orchestrator | 多实例控制、编排管理                                    |      |

> 还有一些外部服务，保证程序间可以正确的协同工作。包括消息总线有KAFKA、MQTT，资源调度 ZOOKEEPER以及监控PROMETHEUS等





#### ☕ 基本概念

- **DeviceMeta**: 设备概式（DNA）
- **Device**: 设备实例（实际装备）
- **Thing**: 监测对象（形成我们的物联网世界）
- **Layout**: 部署与网络配置
- **Dimension**: 监测维度综合面
- **Scheme/Task**: 调度方案与任务执行

> 简单讲，就是 **有设备 -> 有部署 -> 有数据 -> 有计划 -> 有结果！**



![image-20250428141428086](imgs/DAC相关知识分享/image-20250428141428086.png)

#### :link: LINK<-->CHANNEL

+ **LINK** 代表一个物理连接。
+ **CHANNEL** 连接层上的一层封装。协议解析中都是对channel层的读写。

> link在设备连接创建后实例创建。而channel会在任务调度之后创建数据与协议之间的通道。
>
> 所以我们常见的几个错误可以解释道：
>
> `no data listener` ：沙箱启动失败
>
> `no data handler` : 连接未与channel绑定



> `BasicChannel.go`
>
> ```go
> // Rebind 重新绑定连接。 也是在任务启动调度时候，Driver.go中调用GetChannel->NewChannel时绑定link.
> // 如果这里没绑定，就会报错no data handler
> // 例如：任务还没调度，就有报文上来
> func (c *BasicChannel) _Rebind(link api.Link, receiver api.LinkDataHandler) {
> 	if c.link != nil {
> 		c.link.RemoveRef(c)
> 		c.link.RemoveEventListener(c)
> 		c.link.RemoveDataHandler(c.Key())
> 	}
> 	c.link = link
> 	if link != nil {
> 		link.AddRef(c)
> 		link.AddEventListener(c)
> 		link.AddDataHandler(c.Key(), receiver)
> 		go c.OnStatus(link, link.GetStatus(), "") //link 先于 Channel 处理事件, 因此需重新发送一次给 Channel
> 	}
> }
> 
> // 在任务启动调度的时候，沙箱初始化时执行。
> // 如果沙箱启动失败，那么这里就会添加失败，就会报错 no daat listener
> func (c *BasicChannel) AddDataHandler(key string, lsn api.LinkDataHandler) {
> 	c._addDataHandler(key, lsn, c.onPayloadReceived)
> }
> 
> 
> ```
>
> 

#### :taco: 任务调度

任务驱动沙箱，处理设备上报/返回的数据报文，解析出最终的传感器数据: 

![image-20250428142309267](imgs/DAC相关知识分享/image-20250428142309267.png)

某Thing的接入标准动作为: 

+ 将Scheme驱动为任务(实际为一次能力调用);

+ 任务调用”执行器”(Driver);

+ 执行器执行动作;

+ 获得数据或结果;

+ 数据推送;

+ 任务完成.

 

任务主要分为两种

1. **周期采集** 通过`cron`表达式定时执行的任务
2. **监听** 结构物Thing保存之后、或者DAC程序启动的时候一次性启动任务。

![image-20250428144038823](imgs/DAC相关知识分享/image-20250428144038823.png)



:bento: **沙箱**

![image-20250428143141458](imgs/DAC相关知识分享/image-20250428143141458.png)



 **处理器** : 包括了协议处理的整个过程: 开始, 控制, 设备交互, 编解码处理, 结果传输等;

 **上下文** **Context**: 当前处理相关的环境设置: 和设备通讯的存取器, 内存环境, 用量和限制等;

 **支撑库 Library**: 库函数: 编解码, 运算, 通讯等;

 **实现**: 沙箱在 Lua的实现



① **PING**/**PONG模式**

1. 执行模组初始化沙箱, 以保证本次执行是在一个独立的环境 ( 上下文: `ctx`)中.

2. 调用 start 方法;

   ​	a) Start方法内部编码;

   ​	b) 调用 `ctx.ssend`函数, 并按需设定超时;

   ​	c) 等待应答或超时;

   ​	d) 解码;

   ​	e) 调用 `ctx.done`

3. 执行模组回收沙箱资源;

4. 结果处理.

② **沙箱内多次交互**

1. 执行模组初始化沙箱

2. 调用 start 方法;

   a) 编码, 按需循环同步发送多次, 或异步发送多次;

   b) 同步发送: 则等待应答后解码处理;

   c) 异步发送, 则实现 `onData` 方法, 用于进行编解码

   d) 若需要通知数据, 可调用 `ctx.notify` 方法(类似`ctx.done`, 不结束驱动过程)

3. 完成;

4. 沙箱资源回收;

5. 执行器完成.



#### ✨ 沙盒全观

| 功能              | 描述                 |
| ----------------- | -------------------- |
| start             | 开始执行协议         |
| `onData`          | 收到数据后处理       |
| dispose           | 清理资源（随网而上） |
| `ssend`/`asend`   | 同步/异步发送操作    |
| `ctx.done/notify` | 完成或通知数据处理   |

当前上下文中应具备的设备相关信息:

| 属性      | 类型  | 说明     |
| --------- | ----- | -------- |
| device    | Table | 设备属性 |
| protocol  | Table | 协议属性 |
| interface | Table | 接口属性 |