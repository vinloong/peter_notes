用streamlit实现以下功能“

查找所有soip设备的在线状态（从pg数据库）

SELECT a.*, dstdi."deviceId", dc.id, dc."protocolMetaId" dcpmid, cm."protocolMetaId" cmpmid from (
		SELECT t."belongTo", layout."thingId", lnk.id, lnk."parentLinkId",
				lnk."fromDeviceInterfaceId", it.key, im.name, di.properties,
				lnk."toDeviceInterfaceId", di."deviceId", dmi."deviceMetaId"
			FROM "LayoutLink" lnk, "LayoutNode" node ,"Layout" layout,
				"DeviceInterface" di, "DeviceMetaInterface" dmi, "InterfaceMeta" im , "InterfaceType" it, "Thing" t
			WHERE
				layout."thingId" = t.id
				AND lnk."fromDeviceInterfaceId" = di.id AND di."deviceMetaInterfaceId" = dmi.id
				AND dmi."interfaceMetaId" = im.id AND im."interfaceTypeId"=it.id
				AND lnk."fromLayoutNodeId" = node.id AND node."layoutId" = layout.id
		) a
	LEFT JOIN "DeviceInterface" dstdi ON dstdi."id" = a."toDeviceInterfaceId"
	LEFT JOIN "DeviceCapability" dc on dc."deviceInterfaceId" = a."toDeviceInterfaceId"
	LEFT JOIN "CapabilityMeta" cm on cm.id = dc."capabilityMetaId";
    
解析返回的 properties （JSON）、deviceId(string)形成Soip信息字典。
其中：properties提取其中的id (string)和port （int）属性，形成KEY： soip:$port:$id。

依次轮询Soip字典中的数据（可以考虑并发）：
1. 查询物理连接状态(用KEY查)
    root@anxin-m1:/home/anxin# curl 10.244.7.124:8080/soip/get?key=soip:400005:20230838
    >> Key 'soip:400005:20230838' not found
    root@anxin-m1:/home/anxin# curl 10.244.7.124:8080/soip/get?key=soip:40005:20230838
    >> {"Pod":"10.244.4.170:40005","Out":"10.244.0.0:41171","Created":"2025-08-14T06:06:56.568350296+08:00"}
2. 查询Prometheus中在线状态（DAC在线状态）（用DeviceId查询）
    http://218.3.126.49:19090/
    iota_dac_device_status{deviceId='000651ac-26b2-44bb-aba2-bd7c3dd2b454'}
    返回状态1表示在线。 无或者0表示离线。
对比1、2的结果形成表单输出。并显示1、2状态不一致的个数。



kubectl create secret docker-registry zhiwucloud \
  --docker-server=registry.cn-hangzhou.aliyuncs.com \
  --docker-username=lixiaomin@anxinjoy.com \
  --docker-password=u698EK1pk1 \
  --docker-email=lixiaomin@anxinjoy.com \
  -n iota


阅读代码，编写一个Markdown文件《数据上报编程指南》，用中文，包括：
1. http_comm （json模板+上报）
2. mqtt_comm （json模板+mqtt支持介绍）
3. 特殊的上报（通过protobuf）
4. 介绍分布式 Actor的模式实现多客户端上报。（主节点Master读取Kafka分发到各个上报客户端做单独业务处理）

能写成Slidev格式的md格式吗

增加：
0. 总体流程图
   Kafka实时数据-> Master -- --> Slave: Init/Refresh (从数据库更新配置json) --> 解析到自己的配置类(case class XXXConfig) --> 重载upload方法消费Actor消息 --> 拼接数据 --> 上报动作
1. 数据结构介绍（StationData）
2. uploader中几个方法的固定写法： upload(data:StationData)
def init(): Unit

    def upload(data: Array[IotaData]): Unit = {}

    def upload(data: Array[StationData]): Unit = {}

    def upload(data: Array[RealTimeAggData]): Unit = {}

    def upload(data: Array[AggData]): Unit = {}

    def initDebug(): Unit = {} 用于测试
    
    def refresh(): Unit = {} 用于定时刷新配置（也可用于定时.继承自Expiring默认周期10分钟）
3. 上报动作可以使用：
    mqtt-OneMqttClientWrapper
    http-http_comm.httpRequest
    tcp-TcpSender
3. 进阶写法：
    + 完全自定义的上报
        重载uploader。读取、刷新配置 -> 拼接数据 -> 调用上报动作
    + 缓存处理
        自定义Map存储。 如gaoTai_province（见下面补充代码）
    + 查询历史
        查询ES中的历史数据。参见下面getLastData方法代码
    + 掌握技巧
        搜索代码（比如AES算法全局搜索，实现过的）
        
/*
*
* 	id: 测点id
* 	name:测点名称
* 	structure：归属结构物
*	acqTime： 采集时间
*	devicesData：设备数据
*	theme：主题数据
*
 */
@JsonIgnoreProperties(value = Array("theme", "isOk"), ignoreUnknown = true)
case class StationData(var station: Station,
                       acqTime: DateTime,
                       taskId: String,
                       data: Option[Map[String, Any]],
                       deviceDatas: Map[String, DeviceData] = Map(),
                       var state: Int = 0 // 当前数据的告警等级 测点告警大于0,和告警等级保持一致，设备告警=-1,代表存在设备告警
                      ) 
                      
object gaoTai_province extends Logging {
  logIdent = "[甘肃山丹县和高台县数据上报省水利厅API]"
  var sk_caches: Map[skGroup_cache_key, skGroup_cache_value] = Map()

  def upload(data: AggData,
             config: CommUploadConfig,
             subc: CommUploadContent,
             ac: Option[AuthContent],
             batchId: Option[String]): (Boolean, String) = {
    try {
      var p:Map[String, Any] = http_comm.formatParams(data, config.http, ac)

      val appKey = p.getOrElse("appKey", "").toString
      val secret = p.getOrElse("secret", "").toString
      val timestamp = System.currentTimeMillis().toString
      val nonce = generateRandomString(12)
      val sign = getSign(appKey, secret, timestamp, nonce)
      if (sign.isEmpty) {
        throw new RuntimeException("Sign is empty")
      }

      logger.info(s"sign = getSign($appKey, $secret, $timestamp, $nonce)", sign, appKey, secret, timestamp, nonce)

      val extraParams: Map[String, Any] = Map(
        "timestamp" -> timestamp,
        "nonce" -> nonce,
        "sign" -> sign
      )

      p ++= extraParams

      // 三向位移要z,xy 要分开上报
      if (data.station.factor.protoCode == "4003"){
        val zBody = subc.custom.get.get("z").get.toString
        val xyBody = subc.custom.get.get("xy").get.toString
        val formatedZBody = http_comm.formatContent(zBody,p)
        val formatedXYBody = http_comm.formatContent(xyBody,p)

        val (rs1, _, errstr1) = httpRequest(config.http, subc, p, Some(formatedZBody), None, ac)
        val (rs2, _, errstr2) = httpRequest(config.http, subc, p, Some(formatedXYBody), None, ac)

        (rs2,errstr2)

      }else{
        http_comm.uploadAggData(data, config, subc, ac, extraParams)
      }

    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.warn(s"$AggData 上报出错：$ex")
        (false, ex.getMessage)
    }

  }

  def generateRandomString(length: Int): String = {
    val chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    val random = new Random
    val randomString = (1 to length).map(_ => chars(random.nextInt(chars.length))).mkString
    randomString
  }

  def getSign(appKey:String,secret:String,timestamp:String,nonce:String): String = {
    val data = secret + appKey + timestamp + nonce + secret

    try {
      val md5 = MessageDigest.getInstance("MD5")
      val digest = md5.digest(data.getBytes(StandardCharsets.UTF_8))
      val result = new StringBuilder
      for (b <- digest) {
        result.append(String.format("%02x", Byte.box(b)))
      }
      println(result)
      return result.toString

    } catch {
      case ex: Exception =>
        logger.error("sign 计算异常", ex)
    }

    return ""
  }

}
// 获取上一天同一时间的数据
    def getLastData(data: StationData): Option[ThemeData] = {
        // for test
        // return Some(ThemeData(DateTime.now.plusDays(-1), Map("displacement" -> 1.2)))
        // 前后容差一小时
        val lastDay = data.acqTime.plusDays(-1)
        val startTime = lastDay.plusMinutes(-30)
        val endTime = lastDay.plusMinutes(30)
        val themes = StorageConsumer.queryStationData(data.station.id, startTime, endTime)
        val lastDayMills = lastDay.getMillis
        themes.sortBy(a => math.abs(a.acqTime.getMillis - lastDayMills)).headOption
    }
streamlit实现一个快速排查数据问题的工具（ILook);
并用golang实现一个LookProxy代理程序，可以读取服务器上的Kafka日志、读取k8s api。
输入：
设备ID或名称。

从以下几个数据源中查找相关数据。
1. IoT数据库pg (记录Thing、设备、部署基础信息)
2. 监测数据库PG （记录测点（绑定设备）和结构物（对应Thing）信息、监测项信息（检测内容、单位等））
3. 记录传感器数据 Elasticsearch数据
4. 状态记录 Prometheus
5. 通过K8S接口获取容器日志
6. 采集相关的日志库（日志ES）
7. kafka中相关数据记录


点击【诊断】开始分析设备数据问题：
有一个时间范围的输入（默认最近12小时）
1. 查找并展示设备的相关信息
2. 查找设备关联测点及其相关信息
3. 查找设备数据、测点数据
4. 通过LookProxy读取Kafka中数据（用设备ID过滤）
5. 通过【设备连接KEY】 查询ES日志


scala Akka Actor程序一段时间后会出现不处理数据的情况，代码里只有“ actor 发送 capId=”的日志

g._2.groupBy(_.thingId).foreach(tgps => {
                    tgps._2.groupBy(_.capId).foreach(tds => {
                        var capId = s"${tds._2.head.thingId}.${tds._1}"

                        //本地化按照设备id 来actor处理
                        if (!configLoad.isCloud) {
                            capId = s"local.cap=${tds._1}.dim=${tds._2.head.dimensionId}"
                        }

                        logger.info(s" actor 发送 capId=[$capId],len= ${tds._2.length}")

                        val actOpt = actors.get(capId)
                        //logger.info(s"===========kafka parse is over [${tds._2.length}] [${new DateTime().getMillis - now_s.getMillis}ms]")
                        if (actOpt.nonEmpty)
                            actOpt.get ! tds._2
                        else {
                            val act = if (configLoad.isCloud)
                                akkaSystem.actorOf(Props(new ThingActorCloud(capId, consumerGroup, isTrunk, consumer)))
                            else
                                akkaSystem.actorOf(Props(new ThingActor(capId, consumerGroup, isTrunk, consumer)))
                            actors += capId -> act
                            act ! tds._2
                        }
                    })

                })
                
并且查看日志会出现以下这个报错：
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
    
    
    


registry.ngaiot.com/iot/iot-dac:62
registry.cn-hangzhou.aliyuncs.com/zhiwucloud/iota-dac:62


8月 11 10:54:28 iota-m2 kubelet[1424]: W0811 10:54:28.428143    1424 kubelet_pods.go:800] Unable to retrieve pull secret iota/registry-savoir-secret for iota/iota-dac-watcher-79499d664b-nz5

1a6c506-765e-11f0-88c9-c81f66cfe365 ("iota-dac-4_iota(11a6c506-765e-11f0-88c9-c81
8月 11 10:54:00 iota-m2 kubelet[1424]: W0811 10:54:00.425586    1424 kubelet_pods.go:800] Unable to retrieve pull secret iota/zhiwucloud for iota/iota-dac-4 due to secrets "zhiwucloud" not 
8月 11 10:54:00 iota-m2 kubelet[1424]: I0811 10:54:00.425820    1424 kuberuntime_manager.go:499] Container {Name:iota-dac Image:registry.ngaiot.com/iot/iot-dac:62 Command:[] Args:[] Working
8月 11 10:54:00 iota-m2 kubelet[1424]: E0811 10:54:00.427823    1424 pod_workers.go:182] Error syncing pod 11a6c506-765e-11f0-88c9-c81f66cfe365 ("iota-dac-4_iota(11a6c506-765e-11f0-88c9-c81
8月 11 10:54:03 iota-m2 kubelet[1424]: W0811 10:54:03.425780    1424 kubelet_pods.go:800] Unable to retrieve pull secret iota/registry-savoir-secret for iota/gateway-master-567b4f645b-b42n9
8月 11 10:54:15 iota-m2 kubelet[1424]: W0811 10:54:15.427795    1424 kubelet_pods.go:800] Unable to retrieve pull secret iota/zhiwucloud for iota/iota-dac-4 due to secr


# 自动验证按钮
    if st.button("自动验证"):
        if not st.session_state.test.code:
            st.error("请先生成测试代码")
        else:
            with st.spinner("正在执行自动验证..."):
                # 执行当前代码
                execution_result = execute_code(st.session_state.test.code)
                
                # 将原代码、执行结果和预期结果传给大模型
                improved_code = improve_code_with_llm(
                    original_code=st.session_state.test.code,
                    execution_result=execution_result,
                    expected_outputs=expected_outputs,
                    model=selected_model
                )
                
                # 更新代码并执行
                st.session_state.test.code = improved_code
                st.code(improved_code, language="lua")
                
                # 重新执行改进后的代码
                response,result_map = execute_code(improved_code)
                
                if response.status_code == 200:
                    st.success("请求成功！响应内容：")
                    st.json(response.json())
                else:
                    st.error(f"请求失败，错误：{response.text}")
                # 修改要支持多次验证。
                # 最好能够记录每次验证的过程代码、结果。
                # response中的data是返回的数据，response['data']['data']是具体的结果,用它和expected_outputs对比.
                # response中的log是本次执行的日志信息。
                # 对比方法： expected_outputs字典中的每个非空元素的键值对都要在result_map中找到对应的键，并比较值是否相等。（字符串转换成数字对比，并且允许误差）
                # 如果与预期结果不匹配，则执行多次验证，直到满足预期结果为止。 
                # 将本次返回的log信息（如果代码执行错误，会显示具体的报错信息和报错行号）、本次返回的数据（如果有）、期望的数据以及代码一并传感器大模型，让它重新生成协议解析代码。
                # 需要大模型返回完整的代码，以及本次修改的内容。
                # 通过代码解析，读取到这两部分的返回。
                st.json(response)

以下内容是给出的一个目录列表，编写一个markdown文件。
每个公式都可以从目录中链接到指定章节，并编写每个公式的说明。
算法梳理:
1. 设备计算公式、物理量计算公式
    通用振弦计算公式,ε=K*(Fi^2-Fo^2)+Kt*(Ti-To)
    雨量计算
    量水堰渗流计算（尾矿库）
    瞬时流量公式,Q=h*l0*u/1000
    建筑物倾斜率计算,F=tan(θi-θ0)
    倾角温补公式,X=x-AxT^2-BxT-x0;Y=y-AyT^2-ByT-y0;Z=z-AzT^2-BzT-z0
    振动短索索力识别公式,T=4*m*l^2*f1^2/n^2 - n^2*π^2*EI/l^2
    测斜公式,Δl=c*[sin(αi)-sin(α0)]
    压力变送器公式,Δh=(Pi-P0)/(ρ*g)+H0
    渗流计算,1.33[H0+K(F－Fo)/1000]^2.465(m3/s)
    干滩长度计算,l=(H-(h+hi))/s
    三维位移转换公式,X=kx*((y-y0)*sinθ-(x-x0)*cosθ+c1);Y=ky*((y-y0)*cosθ+(x-x0)*sinθ+c2);Z=kz*(z-z0)
    应变计计算轴力,F=A*E*[(∑εi)/n]+F0
    测斜倾角计算挠度,Yn=Yn-1+((Xn-Xn-1)/2)*((An-1*π/180)+(An*π/180)–2*M
    应变计算应力,σ=Eε
    力转应力,σ=F/A
    排放量计算公式,P=Q*C/1e6
    轨心距计算公式,L=(X0*S1)/S-l+L0
    空气数据修正公式,V=k*x+c
    水位插值计算,Δh=(Pi-P0)/(ρ*g)+H0;V=(V2-V1)/(h2-h1)*(Δh-h1)+V1
    三向测斜公式,Δl=c*[sin(αi)-sin(α0)]
    建筑物倾斜率计算,F=tan(θi-θ0)×100%
    雷达水位插值计算,Δh=DAQi-DAQ0;V=(V2-V1)/(h2-h1)*(Δh-h1)+V1
    应变计计算钢筋混凝土支撑轴力(单),"Nc=δ(Ec*Ac/Ei+Ai),δ=εi/Aj"
    干滩长度计算,l=(H-h-(Pi-P0)/(ρ*g))/s
    雷达物位计坡比干滩长度计算,l=(H-(h-hi))/s
    压力变送器公式（带温补）,Δh=(Pi-P0-Kt*(Ti-T0))/(ρ*g)+H0
    激光测距,Δh=(len0-leni)*H/len0
    支撑轴力计算,"Nc=δ(Ec*Ac/Ei+Ai),δ=(∑[k*(fi^2-f0^2)/Aj])/n"
    应变花计算公式,Sr=1/2*E+1/(2*(1-μ))|Sr=1/3*E+1/(3*(1-μ))|Sr=1/4*E+1/(2*(1-μ))|Sr=1/2*E+4/(3*(1-μ))
    应变计计算钢筋混凝土支撑轴力,"N=(Ec*Ac+Es*As)*ε,ε=AVG(εi)"
    输电塔自动调平计算公式,"h(i,j)=[i*a*sin(θ_x) + j*b*sin(θ_y)]/2; d = min{h(i,j)}; H(i,j) = h(i,j) - d; {i=±1,j=±1}"
    应变计计算轴力(组合),F=A*E*[(∑εi)/n]+F0
    钢筋混凝土支撑轴力计算,Nc=[δ(Ec*Ac/Ei+Ai)]/(Ai/n)，δ=(∑δi)/n
    支撑轴力计算(温度平均),"Nc=δ(Ec*Ac/Ei+Ai),δ=(∑[k*(fi^2-f0^2)/Aj])/n;Δt=Δt,Δt=AVG(tv)"
    库水位和干滩计算公式,h=h0+h1-hi;l=(l0+l1-li-h)*l2/(l0-h0)+c
    混凝土温度组合计算,y=concrete_temperature(x)
    
2. 分组公式
    连通管沉降分组
    连通管沉降分组级联
    测斜管内部位移公式
    
3. 振动相关
    振动索力识别公式,T=4*m*l^2*f1^2/1000
    振动特征参数计算,"phy={pv,ppv,rms}"
    振动等效声级计算,"phy={soundLevel,arrAcc}"
    振动积分,"phy={pv,ppv,rms}"
    振动二次积分,"phy=Inter2{pv,ppv,rms}"
    
4. 后计算/过滤识别算法
    滑窗、平滑：
        取中值
        限幅
        滑动平均
        方差判断平均
        滤波算法
        去极值移动平均
        加权滑动平均
        中值平均
        限幅平均

    毛刺识别：
    趋势判断：    

DAC旧顺序
// 通讯服务
	gAS = conn.NewService()
	gAS.Start()
	switch zook.RunMode {
	case zook.Single:
		gAS.Prepare(gDS.GetRootInterfaces(""))
	case zook.Multi:
		tids, err := zook.Service.GetSources()
		if err != nil {
			panic(err)
		}
		for _, tid := range tids {
			gAS.Prepare(gDS.GetRootInterfaces(tid)) // 监听准备
		}
	}

	// SM 协议沙箱
	log.Info("[main] start sandbox manager...")
	sm := driver.NewManager()

	// gSLM 任务/网络/流量监听器
	gSM = stat.NewSysMonitor(iKafkaSyncer)
	gAS.SetLinkEventListener(gSM) //流量/网络

	// gSS 任务调度
	log.Info("[main] start scheme service...")
	gSS = scheme.NewService(conf, gAS, iKafkaSyncer, sm, gSM)
	gSS.StartSite(gDS)


python+streamlit实现:
输入：
1. 一个文本输入框，输入结构物ID列表，输入这种格式 "a","b" （或者a,b 或者a换行b）。一个时间基准（输入时间）。
2. 从pg数据库中查询对应的设备ID。select distinct(d.id) from "Device" d,"CapabilityMeta" cm,"CapabilityProperty" cp  where
d."deviceMetaId"=cm."deviceMetaId" and cm.id=cp."capabilityMetaId" and cp.category='Output' and
"thingId" in ('c25d70dc-cd09-4247-8e60-2093a3219c0b')
3. 根据设备ID从ES中查询最新数据。
    http://localhost:9200/anxincloud_last_raw/_search
    {
  "query": {
    "bool": {
      "must": [
        {"terms": {
          "iota_device": ["c7b67fad-1366-4911-907a-e46c242acfa6",...]
        }}
      ]
    }
  }
}
查询结果：
_source" : {
          "structId" : 5155,
          "iota_device_name" : "表面式应变计1111",
          "data" : {
            "am" : 66.012,
            "frequency" : 1488.398,
            "phy_x" : 19.801,
            "physicalvalue" : 1488.398,
            "temprature" : 21.238
          },
          "collect_time" : "2024-12-10T17:06:00.000+0800",
          "meta" : {
            "am" : "幅值(mV)",
            "frequency" : "频率(Hz)",
            "phy_x" : "信噪比(°)",
            "physicalvalue" : "微应变(με)",
            "temprature" : "温度(℃)"
          },
          "iota_device" : "c7b67fad-1366-4911-907a-e46c242acfa6",
          "create_time" : "2024-12-10T17:06:37.791446257+08:00"
        }
        
        读取到每个设备的最新数据时间collect_time
    
4.  根据基准时间判断，如果在基准时间之后，表示数据在线。

界面中显示所有点位在线或离线的状态。



python实现：
指定目录下有多个json文件
```json
[
  {
    "桥梁唯一编码": 24011,
    "测点唯一编码": "240111",
    "测点编号（对内）": "梁板挠梁1/4左幅01",
    "测点编号": "cc-pld-g05-l01",
    "测点位置": "梁板挠梁1/4左幅01",
    "监测类别": "RES",
    "监测类型": "PLD",
    "监测内容编码": 334,
    "监测内容": "竖向方向变形"
  },
  {
    "桥梁唯一编码": 24011,
    "测点唯一编码": "240112",
    "测点编号（对内）": "梁板挠梁1/4左幅01",
    "测点编号": "cc-pld-g05-l01",
    "测点位置": "梁板挠梁1/4左幅01",
    "监测类别": "RES",
    "监测类型": "PLD",
    "监测内容编码": 334,
    "监测内容": "水平方向变形"
  }
]



```
合并这些json文件生成一个字典，key是【测点编号（对内）】，值为该对象。

你是一个IoT物联网行业专家，并且精通python编程和数据库编程。
用python+streamlit实现
-- 查询pg数据库得到以下数据。
select st.structure_name_cn as 结构物名称,
       st.id as 结构物ID,
       f.safety_factor_type_name as 监测因素名称,
       f.safety_factor_type_id as 监测因素ID,
       s.sensor_location_description as 传感器名称,
       s.sensor_id as 传感器ID
from t_sensor s,t_structure st,t_safety_factor_type f where s.safety_factor_type_id=f.safety_factor_type_id and s.struct_id=st.id and s.is_deleted=false;

-- ID字段都是整型数据，名称字段均为字符串，
-- 实现三级联动的筛选框【结构物 > 监测因素 > 传感器】。最后一级【传感器】支持多选。

-- 选择完传感器之后，点击按钮【查询省平台统计数据】。

-- 查询CK数据库(查询传感器的聚集数据)
-- 输入查询时间范围
select Values from province_data_statistic where MonitorCode='2401137' and CollectTime between '2025-07-30' and '2025-07-31';
Values是Array[Float]字段。
根据首条数据判断该数组的长度，然后依次每列的时间曲线。



@所有人   培训通知：
一、培训主题：后端数据接入流程介绍
二、培训时间：2025年 7 月 28 日 14:00开始
三、培训地点：镇江休息室
四、培训导师：@殷伟文
五、参会人员：后端开发人员、以及感兴趣的各位同事
六、培训流程：
1、后端数据接入流程介绍
2、交流环节
https://doc.weixin.qq.com/doc/w3_Ac4A4wYjAO8CNnDBH4R03RimFq2tx?scode=AAkAvwd3ABE8C02D53Ac4A4wYjAO8
培训后提交培训总结至培训导师抄送研发组长，事关绩效相关分数，请各位协调好手上工作，准时参加，如有请假人员，请提前沟通，谢谢~


采用 1D - CNN 字节统计 + BiLSTM 序列关系 + GraphSAGE 建模。1D - CNN（一维卷积神经网络）用于提取字节级统计特征，通过不同大小的卷积核在协议码流上滑动，提取码流中的局部特征，如字节的频率分布、特定字节模式等，对未知协议进行大类识别，例如区分 Modbus/TCP、HJT212 等常见协议类型。BiLSTM（双向长短期记忆网络）用于捕捉序列关系，协议数据通常具有一定的顺序性和上下文依赖关系，BiLSTM 能够同时从正向和反向对序列进行处理，更好地理解字段之间的先后顺序和语义关联，实现中粒度的结构推理，例如识别字段边界与含义。GraphSAGE（Graph SAmple and aggreGatE）用于对协议数据进行图结构建模，将协议中的字段、帧以及它们之间的关系构建为图结构，通过采样和聚合邻居节点的特征，学习到协议的全局结构特征，进一步提升协议识别的准确性和对复杂协议的处理能力，实现细粒度字段语义还原，完成从协议码流到物模型接口的自动映射。​
协议变体与复杂场景适配​
全流程自动化平台集成​
多场景设备接入验证​
技术成果标准化输出​


有关调用实时(JIT)调试而不是此对话框的详细信息，
请参见此消息的结尾。

************** 异常文本 **************
System.OutOfMemoryException: 引发类型为“System.OutOfMemoryException”的异常。
   在 DAAS.comm.CircleList`1..ctor(Int32 capcity, Int32 page) 位置 m:\Jenkins\workspace\DAAS-DotNet4\DAAS\comm\CircleList.cs:行号 55
   在 DAAS.Sample.SamplingMana..ctor() 位置 m:\Jenkins\workspace\DAAS-DotNet4\DAAS\Sample\SamplingMana.cs:行号 12
   在 DAAS.Sample.SampleChannel..ctor(Channel channel, OscSamSimulateProperty prop) 位置 m:\Jenkins\workspace\DAAS-DotNet4\DAAS\Sample\SampleChannel.cs:行号 57
   在 DAAS.Sample.IsolateSampleControler.Init(ChannelList chlist, Boolean startTimer) 位置 m:\Jenkins\workspace\DAAS-DotNet4\DAAS\Sample\IsolateSampleControler.cs:行号 34
   在 DAAS.MainControlSamOsc.LoadDefault(Boolean startTimer) 位置 m:\Jenkins\workspace\DAAS-DotNet4\DAAS\MainControlSamOsc.cs:行号 135
   在 DAAS.MainControlSamOsc.StartOscFun(Boolean autoStartflag, Boolean silence) 位置 m:\Jenkins\workspace\DAAS-DotNet4\DAAS\MainControlSamOsc.cs:行号 419
   在 DAAS.Views.frmGraph.toolStripButtonOsc_Click(Object sender, EventArgs e) 位置 m:\Jenkins\workspace\DAAS-DotNet4\DAAS\Views\frmGraph.cs:行号 85
   在 System.Windows.Forms.ToolStripItem.RaiseEvent(Object key, EventArgs e)
   在 System.Windows.Forms.ToolStripButton.OnClick(EventArgs e)
   在 System.Windows.Forms.ToolStripItem.HandleClick(EventArgs e)
   在 System.Windows.Forms.ToolStripItem.HandleMouseUp(MouseEventArgs e)
   在 System.Windows.Forms.ToolStripItem.FireEventInteractive(EventArgs e, ToolStripItemEventType met)
   在 System.Windows.Forms.ToolStripItem.FireEvent(EventArgs e, ToolStripItemEventType met)
   在 System.Windows.Forms.ToolStrip.OnMouseUp(MouseEventArgs mea)
   在 System.Windows.Forms.Control.WmMouseUp(Message& m, MouseButtons button, Int32 clicks)
   在 System.Windows.Forms.Control.WndProc(Message& m)
   在 System.Windows.Forms.ScrollableControl.WndProc(Message& m)
   在 System.Windows.Forms.ToolStrip.WndProc(Message& m)
   在 System.Windows.Forms.Control.ControlNativeWindow.OnMessage(Message& m)
   在 System.Windows.Forms.Control.ControlNativeWindow.WndProc(Message& m)
   在 System.Windows.Forms.NativeWindow.Callback(IntPtr hWnd, Int32 msg, IntPtr wparam, IntPtr lparam)




在事件发生前，涉事数据库主要采取了以下几项基础安全措施：
1. 数据库监听的非标准端口 19200，意图避免默认端口被轻易扫描，但此类“安全通过模糊”的措施效果有限。
2. 通过 Nginx 做反向代理，设置了基础的 IP 白名单控制（限制访问源 IP），但由于配置疏漏，未覆盖所有接口路径，导致部分接口仍然可以从公网访问。
3. 服务器对外网提供了基础的防火墙硬件隔离，但是19200处理开放状态造成本次泄漏。
4. 最早ES作为日志数据库不包含敏感信息，所有的用户都有读写权限，但后续业务发展添加了这部分业务库，未能即时审计，未即时加以隔离保护。


/artemis/api/resource/v2/encodeDevice/search  indexCode   [GA序号 deviceCode]
                                                |
                                                |
                                                |
/artemis/api/resource/v1/cameras  encodeDevIndexCode   [cameraIndexCode]

7.23生产力工具
AI助力生产力工具的提升，工具是人类文明最大的贡献者。工具的提升》文明的提升，ai或许还不能代替人类思考，帮助人类实现思考链上信息收集和对比的重要一环。
思考》搜集信息》对比知识》做出决策。关注于决策。类如：点外卖agent：今天想吃啥，AI分析过往记录（习惯、最近记录、预测倾向）--》预测能力，找寻规律。
万事万物均有规律中找寻

01030200B3F9F1
提取一个安心云方面的专利
按课题的那种  创新研究**技术、解决***问题  达到***效果


研发了多物理量的桥梁结构实时轻量化智能监测系统，集成数据清洗和压缩感知技术、边缘-云端协同计算技术，构建了覆盖感知-算法-平台全链条的智能监测体系（见图4-8）。解决了传统桥梁健康状态监测中施工复杂、数据信噪比低、人工依赖度高和长期稳定性差等问题。为工程监测提供了轻量化、智能化的长期在线监测与预警方案，降低了25%的桥梁运维成本。



轻量化，平台+边缘联动采集、控制系统，传感器属性、能力物模型抽象，支持多物理量同步采集和融合感知。
高并发的轻量化采集，脚本化的协议解析能力支持快速定制开发，支持采集能力下层边缘，实现边缘采集分析过滤。自研数据过滤系统，支持多种滑窗过滤算法，解决了采集过程的异常数据， 

继续挖掘。。。


基于桥梁结构特性构建传感器能力与属性的统一物模型，支持应变、振动、位移、倾角、风速、温湿度等多维数据的同步采集与融合感知。并采用轻量化高并发采集架构，边缘端设备脚本化协议解析机制，快速适配多种工业协议，实现高并发、多通道数据采集与接入，极大提升采集效率与适配速度。另外集成自研的数据清洗与滑窗过滤算法模块，支持中值滤波、极值剔除、标准差滑窗判断等算法，有效排除异常采样点。系统通过策略驱动实现采集→边缘预处理→压缩感知→云端建模分析→平台可视化预警的全过程闭环，支持平台统一策略下发与模型迭代。解决传统桥梁监测系统解决原始数据中噪声高、冗余大、人工清洗负担重的痛点，以及云端全量上传导致的数据延迟高、带宽压力大问题；并解决了多协议设备接入困难、开发周期长、运维成本高的问题。实现桥梁结构状态轻量化、低功耗、多物理量融合感知的长期在线监测


后端开发想做一个前端界面，react的，接口后端我自己写，有下面这些：

查询PG数据库 - 设备基础信息、绑定配置、有无滑窗过滤
查询REDIS数据 - 缓存数据是否正常
查询Prometheus以及DacProxy - 监测设备连接
查询ES的日志 - 设备日志
查询ES的数据 - 最新数据、相关（设备。主题。告警数据） 展示数据》
查询Kafka的数据 - 新旧集群2个kafka，有无对应的记录。

界面中，我需要：
1、有登录界面。 且支持本机下次自动登录
2、搜索结构物（用户输入文字，点击搜索，后端接口搜。支持拼音）
3、结构物选择后，显示表格设备清单。支持筛选搜索。
4、分别调用上述查询接口，在页面中展示对应结果：消息列表、图形等。每个查询是异步的，先查到的先返回。其他等待

界面要美观。


            if prop.get("options"):
                content += f"<p><b>可选项：</b>{', '.join(list(map(str, prop['options'])))}</p>"

TypeError: sequence item 0: expected str instance, dict found


滑坡：当前图像内容为摄像头监测画面,判断图像中是否存在滑坡现象。
当前图像内容为摄像头监测画面,判断图像中是否存在坍塌现象。检测桥梁结构整体或局部坍塌事件,包括但不限于桥面断裂、桥墩台沉降、桥体错位、桥板断裂等典型破坏特征.


修改下面方法：
1. 除了返回字节数组，同时下载文件到本地（指定按照日期目录）,文件名通过传入的CameraNo+时间到毫秒+ext后缀
2. 另外启动清理携程。定期清理三个月之前的文件夹

func DownloadImage(imageURL string) ([]byte, error) {
	// 创建HTTP客户端
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // 跳过证书验证
			},
		},
	}
	resp, err := client.Get(imageURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	imageBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return imageBytes, nil
}

识别程序更新了，目前可以用域名访问 https://ai-camera.ngaiot.com/
另外修改了
1. 图片存储到本地用域名访问
2. 改用轻量级的小模型（保留每隔1小时用max模型识别一次）



1、 2.0关系数据库，id int 21亿。 （历史，长期稳定性考虑不足。 放大数据字段类型，梳理在维护2.0项目，协同处理。 新版本分布式存储，固定分片方式不存在该问题，项目前期做好数据量核算）

3、传输积压问题 - idau.tool性能缺陷。 有优化替代品，直接应用。


2、振动DAAS采集 - 产品化的版本不太适用。这个在当前基础上做项目使用的版本（关闭多余功能、文件存储、刷新率降低）。 轻量化版本规划在做，。


软件-袁凤华
1. 未识别到的告警是否可以不发 alarmType:"none"
2. 摄像头LLM识别配置管理（http://222.186.227.196:32145/）要添加个搜索功能
3. 在一体化二参告警测试中（Lora控制盒）现场测试没通过，主要问题为：数据长度不匹配，传输距离限制

方案-王泽林

王瑀轩

摄像头LLM识别配置管理搜索功能 - 殷伟文（暂时不处理）
区域识别提升精度 - 殷伟文 （后续优化7-25）
RTU触发平台识别 - 殷伟文 （后续优化7-31）
识别功能缺陷方案部传递 - @王泽林
工控机基础环境搭建完成 - 今日下班前 @王瑀轩
软硬件现场联调人员安排 - @刘歆毅 @黄莉




airbyte用docker-compose启动报错：airbyte-temporal组件
[Fx] INVOKE             go.temporal.io/server/service/history.ServiceLifetimeHooks()
{"level":"fatal","ts":"2025-07-10T08:44:37.639Z","msg":"ListenIP failed, unable to parse bindOnIP value","address":"10.8.30.143 192.168.1.100","logging-call-at":"rpc.go:228","stacktrace":"go.temporal.io/server/common/log.(*zapLogger).Fatal\n\t/temporal/common/log/zap_logger.go:150\ngo.temporal.io/server/common/rpc.getListenIP\n\t/temporal/common/rpc/rpc.go:228\ngo.temporal.io/server/common/rpc.(*RPCFactory).GetRingpopChannel\n\t/temporal/common/rpc/rpc.go:156\ngo.temporal.io/server/temporal.(*Server).newBootstrapParams.func1\n\t/temporal/temporal/server.go:365\ngo.temporal.io/server/common/resource.MembershipFactoryProvider\n\t/temporal/common/resource/fx.go:266\nreflect.Value.call\n\t/usr/local/go/src/reflect/value.go:543\nreflect.Value.Call\n\t/usr/local/go/src/reflect/value.go:339\ngo.uber.org/dig.defaultInvoker\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/dig.go:439\ngo.uber.org/dig.(*node).Call\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/dig.go:912\ngo.uber.org/dig.paramSingle.Build\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/param.go:240\ngo.uber.org/dig.paramList.BuildList\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/param.go:196\ngo.uber.org/dig.(*node).Call\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/dig.go:903\ngo.uber.org/dig.paramSingle.Build\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/param.go:240\ngo.uber.org/dig.paramList.BuildList\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/param.go:196\ngo.uber.org/dig.(*node).Call\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/dig.go:903\ngo.uber.org/dig.paramSingle.Build\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/param.go:240\ngo.uber.org/dig.paramList.BuildList\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/param.go:196\ngo.uber.org/dig.(*node).Call\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/dig.go:903\ngo.uber.org/dig.paramSingle.Build\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/param.go:240\ngo.uber.org/dig.paramList.BuildList\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/param.go:196\ngo.uber.org/dig.(*Container).Invoke\n\t/go/pkg/mod/go.uber.org/dig@v1.13.0/dig.go:587\ngo.uber.org/fx.(*App).executeInvoke\n\t/go/pkg/mod/go.uber.org/fx@v1.14.2/app.go:873\ngo.uber.org/fx.(*App).executeInvokes\n\t/go/pkg/mod/go.uber.org/fx@v1.14.2/app.go:846\ngo.uber.org/fx.New\n\t/go/pkg/mod/go.uber.org/fx@v1.14.2/app.go:594\ngo.temporal.io/server/temporal.(*Server).Start\n\t/temporal/temporal/server.go:228\nmain.buildCLI.func2\n\t/temporal/cmd/server/main.go:168\ngithub.com/urfave/cli/v2.(*Command).Run\n\t/go/pkg/mod/github.com/urfave/cli/v2@v2.3.0/command.go:163\ngithub.com/urfave/cli/v2.(*App).RunContext\n\t/go/pkg/mod/github.com/urfave/cli/v2@v2.3.0/app.go:313\ngithub.com/urfave/cli/v2.(*App).Run\n\t/go/pkg/mod/github.com/urfave/cli/v2@v2.3.0/app.go:224\nmain.main\n\t/temporal/cmd/server/main.go:50\nruntime.main\n\t/usr/local/go/src/runtime/proc.go:255"}



A4-0C-66-1C-OF-B4
a4:0c:66:1c:of:b4

      - DB=postgresql
      - DB_PORT=5432
      - DYNAMIC_CONFIG_FILE_PATH=config/dynamicconfig/development.yaml
      - LOG_LEVEL=${LOG_LEVEL}
      - POSTGRES_PWD=postgres
      - POSTGRES_SEEDS=pg-210
      - POSTGRES_USER=postgres


docker run --rm \
  --name airbyte-temporal2 \
  -e DB=postgresql \
  -e DB_PORT=5432 \
  -e DYNAMIC_CONFIG_FILE_PATH=config/dynamicconfig/development.yaml \
  -e LOG_LEVEL=INFO \
  -e POSTGRES_PWD=postgres \
  -e POSTGRES_SEEDS=10.8.30.182 \
  -e POSTGRES_USER=postgres \
  airbyte/temporal:0.50.4



record parse error: {"notificationType":"voice","receiver":[{"name":"wangjiahui","phone":"13776310625"}],"content":{"title":"","body":"动态采集FS-V08（测试部刘纯）于2025-07-10 23:19触发3级告警，请登录平台及时处理。"},"time":"2025-07-10T23:19:51.586+0800","category":"alarm"}

to: voice receivers:[name:wangjiahui phone:13776310625] title: content:分布式数据采集节点wjh于2025-07-10 23:10触发2级告警，请登录平台及时处理。


{
    "userId": "ce2d7eb2-e56e-422e-8bbe-95dfa18e32f8",
    "thingId": "effc73fb-b5be-4e98-a270-0f50f497f83f",
    "dimensionId": "1fbcd5fc-2347-493f-92a4-a1d3fd3252e4",
    "dimCapId": "23677a83-cb29-4a95-8f95-985c823cd5bb",
    "capId": "acb3b28c-aafa-4c55-8989-5fbac75a4d15",
    "deviceId": "b96e0445-729b-4285-b95f-a32f05566dc3",
    "scheduleId": "57e96cf5-2863-49de-9c3e-9dd2f80c2d21",
    "taskId": "36c31449-cd42-4258-bc6c-5b83d91434de",
    "jobId": 1,
    "jobRepeatId": 1,
    "triggerTime": "2025-07-10T23:35:00+08:00",
    "realTime": "0001-01-01T00:00:00Z",
    "finishTime": "2025-07-10T23:35:43.929163476+08:00",
    "seq": 0,
    "released": false,
    "data": {
        "type": 1,
        "data": {
            "humidity": 52.71,
            "temp": 31.45
        },
        "result": {
            "code": 0,
            "msg": "",
            "detail": null,
            "errTimes": 0,
            "dropped": false
        }
    }
}


{
    "model": "recalc",
    "stationId": [
        121558,121562,121535,121575,121563,121568,121559,121570,121577,121578,121567,121580,121571,121566,121536,121561,121547,121564,121548,121549,121565,121576,121550,121569,121581,121572,121579,121573,121574,121582,122002,122064,122075,121999,122042,122043,122039,122045,122061,122003,121998,122001,122011,122070,122009,122063,122000,122010,122056,122012,122021,122066,122013,122014,122031,122018,122015,122052,122017,122054,122060,122068,122016,122062,122065,122019,122020,122023,122022,122038,122024,122057,122072,122025,122026,122048,122027,122028,122030,122032,122029,122051,122049,122055,122067,122033,122069,122044,122046,122041,122050,122053,122034,122036,122059,122037,122058,122071,122047,122073,122074,122035,122040
    ],
    "msgId": "b602cdde-d8cd-4fdc-a9db-dd32b809bd66",
    "dataBeginTime": "2025-05-14T00:00:14.000Z",
    "dataEndTime": "2025-06-08T16:00:14.000Z",
    "operation": "add"
}

mqtt.datahub.anxinyun.cn

$up/20230710
  � �	!$ \{"ID":"20230710","AT":"2025-07-08 09:33:34","VER":"V1.052","IAP":2,"ERR":0,"CV":12,"SAVE":0}��END


[device_monitor]10:57:16 SELECT 1 FROM "public"."device_monitor" WHERE "collector_id"='' AND "collector_time"='';
[device_monitor]10:57:16 UPDATE "public"."device_monitor" SET "collector_time" = , "data" = null WHERE "collector_id" = ;
[device_monitor]10:57:17 mqtt body:{systemNum=S1_G1_GT10054_PV:yuxinxincailiao, value=0.24000000953674317, time=1751514693383}
[device_monitor]10:57:17 SELECT 1 FROM "public"."device_monitor" WHERE "collector_id"='' AND "collector_time"='';
[device_monitor]10:57:17 UPDATE "public"."device_monitor" SET "collector_time" = , "data" = null WHERE "collector_id" = ;
[device_monitor]10:57:17 mqtt body:{systemNum=QT481-9:zhongxinhuagong, value=0.0, time=1751511361110}
[device_monitor]10:57:17 SELECT 1 FROM "public"."device_monitor" WHERE "collector_id"='' AND "collector_time"='';
[device_monitor]10:57:18 UPDATE "public"."device_monitor" SET "collector_time" = , "data" = null WHERE "collector_id" = ;
[device_monitor]10:57:18 mqtt body:{systemNum=TI103B:lichangrongxincailiao, value=0.0, time=1751511321126}
[device_monitor]10:57:18 SELECT 1 FROM "public"."device_monitor" WHERE "collector_id"='' AND "collector_time"='';
[device_monitor]10:57:18 UPDATE "public"."device_monitor" SET "collector_time" = , "data" = null WHERE "collector_id" = ;

ubuntu/root

golang实现一个设备接收和解析工具：
通过UDP通信，需要配置一个端口以及设备的IP和端口。

实现：
查询版本号
查询序列号
 查询主机硬件参数
 查询主机扫描参数
 
 并通过golang web-templagte显示参数信息以及数据。
 
 
 FBG光纤光栅采集仪中：波长模式、调试模式

topic: ai/alarm/FQ6615540

payload: {"deviceId":"FQ6615540","alarmTime":1751334422,"alarmType":"landslide","photoPath":"M123145/20250701/202507011009001.jpg"}




modbus-rtu系数拟合何如应用
其他标准协议匹配（环保、RTU等）
协议识别自动化Agent实现解析代码部署
协议自动化生成解析代码并验证执行流水线


base64.StdEncoding.EncodeToString(imageBytes)
golang实现功能上传图片：

8.2获取API接口认证信息
8.2.1请求地址
/authorize
8.2.2请求方式
POST
8.2.3请求头
ContentType:application/json;charset=utf-8
8.2.4请求体
{
"username":"warning",
"password":"RjJmZjArRndkYXlmYUJDcU9kNmR6QT09"
}
8.2.5响应体
  - StatusCode=200:  OK   响应数据格式: json
{
"status":200,
"result":{
"token":"string", //请求认证字符串
"expire":7200 //认证字符串过期时间
}
}
  - StatusCode=400：Bad Request  响应数据格式：json
{
"show": true,
"msgCn": "",//错误描述
"time": "2019-06-0416:36:43",//请求响应时间
"error": "BAD_REQUEST",// 错误名称
"msgEn": "Failed",//错误描述
"status": 400//请求响应码
}

8.3上传灾害照片
8.3.1传输条件
发生灾害报警时，上报灾害报警照片。
8.3.2请求方式
POST
8.3.3请求地址
/upload/photo
8.3.4请求头
ContentType:multipart/form-data;charset=utf-8
8.3.5参数说明
字段
类型
非空
字段含义
file
file
是
照片文件流（文件名即2.4的照片名称）
path

string
是
相对路径，
格式“{网关ID}/yyyy”

8.3.6 HttpResponse
StatusCode
说明
200
正常上传
401
无效令牌
400
请求参数不正确
500
异常


golang windows程序监控服务开发。
用web-template实现前端界面，可以实现：
1. 前端界面查询所有进程是否启动、启动时间
2. 可增删改启动任务ProcessInfo。 （其中path可以通过选中本地文件的方式从对话框选中）
3. 可进入维护状态，暂时关闭进程检查。但是维护状态只能保持一段时间（比如10分钟，然后自动恢复成监控状态）

func StartNewCheck() {
	imageMap := GetProcess()
	for _, pInfo := range conf.ProcessInfo {
		pName := pInfo.Name
		pPath := pInfo.Path
		delay := pInfo.Delay
		pIdArr, isExist := imageMap[pName]
		if isExist {
			logger.Info("[%s]正在运行", pName)
			if len(pIdArr) > 1 {
				logger.Info("[%s]有多个实例运行，需关闭", pName)
				for _, pId := range pIdArr {
					logger.Info("[%s]准备关闭[%d]", pName, pId)
					kErr := killProcess(pId)
					if kErr != nil {
						logger.Error("[%s]关闭[%d]失败,err=%v", pName, pId, kErr)
					}
				}
			}
		} else {
			logger.Info("[%s]未运行", pName)
			SendEmail(fmt.Sprintf("[%s]未运行\n", pName))
			command := exec.Command("cmd", "/C", pPath)
			runErr := command.Run()
			if runErr != nil {
				logger.Error("[%s]启动失败,err=%v", pName, runErr)
			} else {
				time.Sleep(time.Duration(delay) * time.Second)
			}
		}
	}
}

func GetConfig() *entity.StartConfig {
	var config entity.StartConfig
	if yamlFile, err := ioutil.ReadFile("conf.yaml"); err != nil {
		logger.Error("读取conf.yaml文件失败，err=%v", err)
	} else {
		err = yaml.Unmarshal(yamlFile, &config)
		if err != nil {
			logger.Error("转换yamlFile失败，err=%v", err)
		} else {
			logger.Info("读取配置成功")
		}
	}
	return &config
}

type StartConfig struct {
	ProcessInfo []PInfo    `yaml:"PInfos"`
	TickerTime  int        `yaml:"Ticker"`
}

type PInfo struct {
	Name  string `yaml:"Name"`
	Path  string `yaml:"Path"`
	Delay int    `yaml:"Delay"`
}




2475702f32323530343731310a000100018006001007e9061a173a3b01005b7b224944223a223232353034373131222c224154223a22323032352d30362d3
2362
032333a35383a3537222c22564552223a2256312e303431222c22494150223a322c22455252223a302c224356223a362c2253415645223a307db3bf454e44

2464772f32323530343731310100010001000007e9061a173a000100747b224154223a22323032352d30362d32362032333a35383a3030222c224356223a362c224d4435223a226135383966383837366235383266386163333063373934656635636164353964222c22504e223a312c225543223a302c225553223a22315f315f31222c22695a443038223a6e756c6c7dc8ce454e44

./dtu-benchmark server 14006 14007
.\dtu-benchmark.exe client COM2 1 http://218.3.126.49:14007


INSERT INTO public.t_alarm_link (id, strategy) VALUES (9, '{"name": "固原监控中心-5236", "actions": [{"type": "iot-cap", "parameters": {"body": {"param": "1", "thingId": "03ee26c6-062a-4ae7-8b10-c41b45d739cb", "timeout": 10000, "deviceId": "3aeaf7af-75f0-486b-b21f-f2c7ede00389", "dimCapId": "f3bb4300-32dd-4815-8a8b-b294ff46c809"}}}], "triggers": [{"status": [1, 2], "structure": 5236, "categories": [2]}]}');


{"name": "固原监控中心-5236", "actions": [{"type": "iot-cap", "parameters": {"body": {"param": "1", "thingId": "03ee26c6-062a-4ae7-8b10-c41b45d739cb", "timeout": 10000, "deviceId": "3aeaf7af-75f0-486b-b21f-f2c7ede00389", "dimCapId": "f3bb4300-32dd-4815-8a8b-b294ff46c809"}}}], "triggers": [{"status": [1, 2], "structure": 5236, "categories": [2]}]}
{"name": "总公司监控中心-5225", "actions": [{"type": "iot-cap", "parameters": {"body": {"param": "1", "thingId": "aafa77a4-470c-4f3b-9108-2acfc138b2e9", "timeout": 10000, "deviceId": "ad282960-c5cd-44cb-baef-a4fcf1f843c4", "dimCapId": "f3bb4300-32dd-4815-8a8b-b294ff46c809"}}}], "triggers": [{"status": [1, 2], "structure": 5225, "categories": [2]}]}



iot-cap http://10.244.152.247:7007/_iota_api/capabilities/invoke {"param":"1","thingId":"effc73fb-b5be-4e98-a270-0f50f497f83f","dimCapId":"f3bb4300-32dd-4815-8a8b-b294ff46c809","deviceId":"f7f533fd-ac89-4086-ab8f-070ecf919f43","timeout":10000}


2025-06-27 13:05:39,351 INFO  com.freesun.alarm.model.LinkAction$                           - 发送19:声光报警测试告警失败，内容为：Map(param -> 1, thingId -> effc73fb-b5be-4e98-a270-0f50f497f83f, dimCapId -> f3bb4300-32dd-4815-8a8b-b294ff46c809, deviceId -> f7f533fd-ac89-4086-ab8f-070ecf919f43, timeout -> 10000)，地址为：http://10.244.152.247:7007/_iota_api/capabilities/invoke 响应: 500
2025-06-27 13:05:40,157 INFO  com.freesun.alarm.StreamingJob$                               - received 



golang实现一个工具，实现DTU传输数据丢包和延迟的测试，分客户端和服务端。
客户端连接串口，服务端开启一个tcp端口监听。
需要能测出丢包和延迟。
并通过web显示出来（golang web template）

alarm.msg.template.sms=告警通知：${req.alarm.extras.structureName}于${req.alarm.endTime:yyyy-MM-dd HH:mm}在${req.alarm.sourceName}产生${req.alarm.currentLevel}级告警，${over_threshold.name}测量值${over_threshold.value}${over_threshold.unit}超阈值范围${over_threshold.range}，请关注并防范
templateContent=告警通知：${name}于${time}在${location}产生${warning_level}级告警，${point_name}测量值${value}超阈值范围${threshold_range}，请关注并防范
templateCode=SMS_489075045
templateName=交控工程技术

INSERT INTO MY_TABLE(`(dm.name, model, dm.id)`) VALUES ('(\'爆闪警示装置\', \'XH-BST01\', \'15b6737c-cba5-4a9f-8aa5-24686892380c\')');
da0c7032-9405-4924-b72e-9dbdc758bc23

用正则去识别到
传感器测量值超过1级告警阈值:水位采集值:27.03m,超1级阈值[26.0~+];温度采集值:39℃,超1级阈值[30.0~100]

组装的地方：
                  content = s"xxxxxxxx:${fac.name}采集值:$m${fac.unit_name},超${overths.get.level}级阈值[${overths.get.rangeText()}]"
                  
                  
                  
                  
                  

if st.session_state.latest_image is not None:


rerun("未接收到任何图像",False)

根据不通的页面展示数据
with st.sidebar:
    selected = option_menu(
        "菜单",
        ["实时数据", "配置", "关于"],
        icons=["star", "magic", "info"],
        menu_icon="cast",
        default_index=0,
    )

# 根据选择调用相应的页面函数
if selected == "实时数据":
    page_data.render()
elif selected == "配置":
    page_set.render()
elif selected == "关于":
    page_about()

其中page_data.render()有实时刷新：
time.sleep(0.2)
    st.rerun()
    
每次切换回到实时数据界面时候，下面还有之前显示的内容。


INSERT INTO MY_TABLE(`(dm.name, model, dm.id)`) VALUES ('(\'爆闪警示装置\', \'WDM-XSP-P10-RG\', \'85d50ac0-cfea-4cf2-8c0f-4a5d72a55384\')');
d78cabf4-da1c-473f-8bd3-e14b202d9ffc

编写告警联动说明：
目前告警进程中的告警联动能力分2种：
1. 代码固定的，读取DeviceAConfig.json
    只针对告警产生。
    {
    "iotaDeviceMetaId": "d6f6cb30-5eb0-44a7-928e-6b95daad89f3", // 设备原型ID
    "capMetaId": "f3bb4300-32dd-4815-8a8b-b294ff46c809", // 能力原型ID
    "param": "1", // 能力调用参数
    "filter": {
      "categories": [
        2
      ],
      "status": [
        1,
        2
      ]
    }
  }
    过滤条件支持（根据下面类定义编写具体含义）：
    case class AlarmConditionFilter(structures: Array[Int],
                                types: Array[String],
                                categories: Array[Int],
                                status: Array[Int],
                                states: Array[Int],
                                levels: Array[Int] = null
                               )
2. 通过数据库t_alarm_link配置
    {"name": "固原监控中心-5225", "actions": [{"type": "iot-cap", "parameters": {"body": {"param": "1", "thingId": "03ee26c6-062a-4ae7-8b10-c41b45d739cb", "timeout": 10000, "deviceId": "3aeaf7af-75f0-486b-b21f-f2c7ede00389", "dimCapId": "f3bb4300-32dd-4815-8a8b-b294ff46c809"}}}], "triggers": [{"status": [1, 2], "structure": 5225, "categories": [2]}]}
    名称name，触发条件triggers，执行动作actions
    可以有多个触发条件，多个条件是“与”的关系。同时必须配置time_window表示多长时间窗口内的条件可以联合触发。如：{
    "name": "AI摄像头联动",
    "time_window": "2 hours",
    "triggers": [
      {
        "source": "abc",
        "alarm_type": "3007"
      },
      {
        "source": 2,
        "alarm_type": "2031"
      }
    ],
    "actions": [
      {
        "type": "iot-cap",
        "parameters": {
          "body": {
            "deviceId": "12345678980"
          }
        }
      }
    ]
  }
    触发过滤条件支持（根据下面类定义编写具体含义）：
    case class LinkTrigger(structure: Option[Int],
                       source: Option[String],
                       types: Option[Array[String]],
                       categories: Option[Array[Int]],
                       status: Option[Array[Int]],
                       states: Option[Array[Int]],
                       levels: Option[Array[Int]] = None
                      )

    动作目前仅支持：iot-cap 表示以太能力调用

object AlarmStatus extends Enumeration {
    type AlarmStatus = Value
    // 告警待确认
    val AlarmToConfirmed = Value(1)
    // 告警确认
    val AlarmConfirmed = Value(2)
    // 告警未通过
    val AlarmRefused = Value(3)

    // 是否审核通过（或无需审核）
    def isConfirmed(status: Option[Int]): Boolean = {
        status.isEmpty || status.get == AlarmConfirmed.id
    }

    // 是否为待审核状态
    def isToConfirmed(status: Option[Int]): Boolean = {
        status.nonEmpty && status.get == AlarmToConfirmed.id
    }
}
object AlarmState extends Enumeration {
    type AlarmState = Value

    // 新告警
    val Creation = Value(0)
    // 次数更新
    val CountUpgrade = Value(1)
    // 等级提升
    val LevelUpgrade = Value(2)
    // 自动恢复
    val AutoElimination = Value(3)
    // 手动确认恢复
    val ManElimination = Value(4)
    // 已恢复待确认
    val EliminationToConfirmed = Value(5)

    val unConfirmedStates: Seq[Int] = Seq(Creation.id, CountUpgrade.id, LevelUpgrade.id)

    def AutoEliminateState(alarmType: AlarmType): AlarmState = {
        if (alarmType.upgradeStrategy.exists(_.auto_elimination_need_confirm))
            EliminationToConfirmed
        else AutoElimination
    }
}

object AlarmCategory extends Enumeration {
    type AlarmCategory = Value

    // 设备类告警
    val Device = Value(1)
    // 数据类告警
    val Data = Value(2)
    // 开发类告警
    val Development = Value(3)
    // 系統类告警
    val System = Value(4)
}

object AlarmSourceType extends Enumeration {
    type AlarmSourceType = Value

    // 设备
    val Device = Value(1)
    // 测点
    val Station = Value(3)
    // 系統
    val System = Value(4)
}

我想实现一个vsdx（viso）索引查询界面。用python+streamlit实现.
pg数据库存储记录，包括：
文件名(.vsdx)
sheet

AI滑坡识别 通过运动识别滚石和滑坡，完成初步检测 黄昊  6.27
无源标靶 1.基点计算功能支持     卢凯 6.20
无源标靶 2.矩阵变换 计算支持  卢凯  6.27
无源标靶 上位机联调调试 殷伟文   6.27
协议破解 针对ModBus 标准协议格式的报文，基于多条结果拟合出对应的计算关系及系数  张玉祥  6.25
协议破解 协议自动化生成解析代码并验证执行流水线  殷伟文 6.30

AI技术应用宣讲之IoT 平台协议融合解析基础及AI Agent 在协议识别中的应用。
1. 首先介绍IOT平台上协议的作用
> 什么是协议？
协议--抽象为脚本代码，控制与设备的交互、编解码、数据输出及其他行为。IOT平台将采集模型抽象为接口、能力和属性。【协议】是能力的重要组成部分。目前平台有xxx种设备协议（多于1000多种设备原型），一个设备可能有多个协议，例如采集+控制。
协议之于设备，相当于xxx。 
协议中一般包括：【位置识别（设备编号、通道） 数据说明（数据位、什么数据、范围、单位） 其他校验（长度、安全校验（如CRC校验）】（重新组织描述内容）

> 传感器设备协议分类:
大千世界，千人千面
...

> 为什么没有标准协议：
...

> 如何进行协议接入：
IOT平台是通过采集程序DAC驱动Lua脚本进行协议解析的。当前协议开发流程
【已知协议结构】设备协议说明文档 >> 协议开发 >> 协议编解码 【设备码流】

> 如何提高协议接入效率：
次世代（或者叫别的）：
{
    动态脚本（高灵活性）
    可视化编辑（低代码开发）
    经验丰富的开发人员 （anyway）
}
AI时代：我们能做什么
a) 自动生成
【协议文档】 >> 协议开发 <<（多次）>> 编解码验证
b) 协议识别（逆向工程）
【设备码流】 >> 分析码流 >> 协议结构 >> 协议开发

2. AI Agent 在协议中的应用
a) 协议识别
协议识别：从传感器传输码流中识别协议。
应用场景：在缺少协议说明文档，如年久失修过期产品，文档丢失，第三方运维等场景。
实现方法以及AI的作用： 码流捕获 >> AI分析结构 >> 匹配现有协议库 、标准协议解析 、 AI直接分析协议格式

难点： 目前市面上没有专攻类似领域的大模型。协议种类繁多（交互复杂、变长、自定义转换），或协议加密。
我司优势：协议储备
目前进展：
    我司协议库特征库建立 （协议通常有固定的帧头（Header）、帧尾（Footer）或特定控制字段，这些就是协议特征）
    标准协议Modbus-RTU协议码流解析
    协议识别应用（支持与我司协议库匹配以及标准协议解析）
后续规划：
    总体方向：提升准确性、适用范围。完善应用形态，推广使用。
    完善Modbus-RTU逆向到协议生成（6月）
    码流特征机器学习（原始码流输入神经网络（如 CNN、LSTM），自动学习协议特征）
    非标准协议识别方法（完全AI 驱动的协议逆向工程​​：自动从未知码流中推断协议结构）

b) 协议生成
协议生成：应用于正向工程，使用文本生成大模型生成IOT平台协议脚本代码。
应用场景：目前协议开发流程提效（低代码或零代码）
实现方法以及AI的作用：设备协议文档 >> AI解析提炼 >> AI生成 >> 协议自动校验+AI修正（重复）。具体：
    协议文档解析Qwen-long >> 特征提取(提取类型、厂家、型号、参数、输出、示例) >> (协议编写规范、根据类型找样例脚本) >> 大模型生成（Claud.ai） >> 在线校验
难点： 。
我司优势：协议储备；脚本化协议；
目前进展：完成文档输入到脚本生成的全流程开发
后续规划：
    
    





streamlit程序实现无源标靶光电挠度仪配置
现有图片接收和展示代码：

while True:
    try:
        msg = msg_queue.get(timeout=1)  # 每秒等一次，新数据就立即处理
    except queue.Empty:
        continue

    if msg.get("cmd") == "image" and "values" in msg:
        vals = msg["values"]
        img_b64 = vals.get("image")
        timestamp = vals.get("time", "")
        try:
            img_data = base64.b64decode(img_b64)
            img = Image.open(BytesIO(img_data))
        except Exception as e:
            st.error(f"图片解析失败: {e}")
            continue

        # 更新界面
        img_placeholder.image(img, caption=f"时间: {timestamp}", use_container_width=True)
        time_placeholder.text(f"最后更新时间：{timestamp}")

      
记录标靶区间，可以有多个标靶区间。 并且可以点击【新增标靶】，切换到streamlit_drawable_canvas的st_canvas后rect选择，选择结束后，输入测点编号和名称。
当鼠标选中某个rect之后，显示该测点的编号和名称


还有一些其他参数需要设置：
{
    "_from": "dev",
    "cmd": "getPoints",
    "values": {
        "targets": {
            "0": {
                "rectangle_area": {
                    "x": 49,
                    "y": 371,
                    "w": 113,
                    "h": 91
                },
                "threshold": {
                    "binary": 128,  //二值化阈值 0-255
                    "gauss": 9    //高斯模糊阈值  0，3，5，7，9，11
                },
                "radius": 20.0,
                "id": 0,
                "desc": "",
                "base": false
            }
        }
    }
}
首次所有测点信息从tcp获取。

【保存设置】将数据通过tcp下发到设备，并等待设备返回
{
    "_from": "setup",
    "cmd": "setPoints",
    "values": {
        "targets": {
            "0": {
                "id": 0,
                "desc": "0_biaoba",
                "base": false,
                "radius": 20.0,//标靶圆物理半径（计算偏移的转换依据，要精确）
                "threshold": {
                    "binary": 150,
                    "gauss": 5
                },
                "rectangle_area": {
                    "x": 49,
                    "y": 371,
                    "w": 113,
                    "h": 91
                }
            }
        }
    }
}

返回：
{
    "_from": "dev",
    "cmd": "setPoints",
    "values": {
        "operate": true
    }
}

数据存储在：
select * from t_alarm_link;表里

{"name": "多结构物声光告警", "actions": [{"type": "iot-cap", "parameters": {"body": {"param": "1", "thingId": "123456", "timeout": 10000, "deviceId": "12345678980", "dimCapId": "223344"}}}], "triggers": [{"status": [1, 2], "structure": 1, "categories": [2]}]}
如这个示例：
结构物1(strucutre=1)产生数据类型(category=2)时触发执行以太能力调用(type=iota-cap),执行调用设备能力，需要传入设备ID等参数（thingId归属ThingID、DimCapId能力ID等）

修改进程：安心云告警 Alarm-Flink
代码：FS-Anxinyun\trunk\codes\services\alarm
jenkins：https://jenkins.ngaiot.com/job/alarm-flink/











postURI := "/artemis/api/resource/v1/cameras"
	postBody := "{\"pageNo\":1, \"pageSize\":1000}"
	postResponse, err := HTTPPost(postURI, postBody, 30)
	if err != nil {
		fmt.Printf("POST request failed: %v\n", err)
		return
	}
    
返回：
{
  "code": "0",
  "data": {
    "list": [
      {
        "altitude": null,
        "cameraIndexCode": "451bdcca02884e98859820ddda686ada",
        "cameraName": "IPdome",
        "cameraType": 0,
        "latitude": null,
        "longitude": null,
        "updateTime": "2025-05-28T14:26:15.040+08:00",
        "viewshed": null
      }
    ],
    "pageNo": 1,
    "pageSize": 10,
    "total": 1
  },
  "msg": "SUCCESS"
}

查询所有


golang 创建一个摄像头LLM识别配置库（ pg库）,包括摄像头基础配置、大模型检查周期、LLM模型类型（滑坡、collapse、fire等）、或者自定义提示词，是否启用。

create table camera_llm
(
    id               serial,
    type              varchar(16) not null, // 设备类型：yingshi hikisc
    device_id         varchar(128), // 设备ID
    serial_no         varchar(128), // 设备序列号
    name              varchar(128), // 名称
    model              varchar(128), // 型号
    channel_no        varchar(128), // 通道号 默认1
    interval_sec    int,
       ...
);
通过GORM实现。
并用golang web template实现一个配置界面，列表展示这些信息。
web中包括新建方法，新建的时候选择类型，
如果是yingshi,只需要输入序列号。
如果是Hikisc，则选择一个序列号。其他信息自动获取。（golang有GetHikCamIds方法获取所有待配置序列号、GetHikCamInfo获取其他详细信息）
还需要配置检查周期、模型方法、自定义提示词、是否启用。


golang 改造以下方法：

// SendCapData 发送能力数据
func (s *SyncService) SendCapData(dcd *pub.DimCapData) error {
	if dcd.Data == nil {
		dcd.Data = &pub.RawData{Result: &pub.RawDataResult{Code: errors.Unknown}}
	}
	if dcd.Data.Result == nil {
		dcd.Data.Result = &pub.RawDataResult{Code: errors.Normal, Msg: ""}
	}
	if s.notifier != nil {
		go s.notifier.OnSendingCapData(dcd)
	}
	if s.limiter != nil && s.limiter.Limit(dcd) != nil {
		return nil
	}
	return s.cdc.CapDataConsume(dcd)
}


// DimCapData 维度数据
type DimCapData struct {
	UserID      string    `json:"userId"`
	ThingID     string    `json:"thingId"`
	DimensionID string    `json:"dimensionId"`
	DimCapID    string    `json:"dimCapId"`
	CapID       string    `json:"capId"` // CapabilityID
	DeviceID    string    `json:"deviceId"`
	ScheduleID  string    `json:"scheduleId"` // = schemeId
	TaskID      string    `json:"taskId"`     //
	JobID       int       `json:"jobId"`
	JobRepeatID int32     `json:"jobRepeatId"`
	TriggerTime time.Time `json:"triggerTime"`
	RealTime    time.Time `json:"realTime"`
	FinishTime  time.Time `json:"finishTime"`
	Sequence    int       `json:"seq"`      // 同一个 JobRepeatID, 返回多个数据时的顺序号, 0开始.
	Released    bool      `json:"released"` // Thing 是否 为 Released 状态
	// Data 协议返回数据, 格式: CapAck
	Data *RawData `json:"data"` // RawData
	Key  string   `json:"-"`    // 发送给kafka的Key
}

创建一个映射表，UserID

一串或多串未知协议的码流，在知道协议解析结果的情况下，尝试分析协议逻辑。  
首先聚焦modbus协议：



{"type":"register","instance":"10","key":"soip:40001:24125366","ip":"10.244.2.171"}
{"type":"register","instance":"10","key":"soip:40001:25076132","ip":"10.244.2.171"}

DISPATCH_ENABLE=true
DISPATCH_KAFKA=anxin-m1:6663,anxin-m1:6664,anxin-m1:6665,anxin-m1:6666
DISPATCH_USERS=ce2d7eb2-e56e-422e-8bbe-95dfa18e32f8:anxinyun_data_test,c9989314-4834-479b-bbdc-009a686b21c3:savoir_data_test


df -h /kafka-logs/
free -h
cat /var/log/kafka/server.log.2025-05-28-18 | grep ERROR | tail -n10
cat /var/log/kafka/server.log | grep -i zookeeper
cat /var/log/kafka/server.log.2025-05-28-* | grep -i zookeeper
cat /var/log/kafka/server.log.2025-05-28-* | grep -i controller
tail /var/log/kafka/server.log.2025-05-28-22 -f
top -c

htop

iostat -x 1

sudo bmon -p eno3

sudo iotop -aoP

pidstat -d 1 5

dstat -cdngy --top-io --top-bio

ID,f.ID,FactorID,Name,TableName,Items,ItemNames,ItemDecimalPlaces,ItemUnits,f.Project,PlatformFactorId,FactorParent,FactorType
axy2.0:14217,axy2.0:137,137,车流量监测,T_THEMES_TRAFFIC_LOAD_SIMPLIFY,"GrossWeight,AxieWeight1,AxieWeight2,AxieWeight3,AxieWeight4,AxieWeight5,AxieWeight6,AxieWeight7,AxieWeight8,AxieWeight9,AxieWeight10","车重,轴1重,轴2重,轴3重,轴4重,轴5重,轴6重,轴7重,轴8重,轴9重,轴10重","2,2,2,2,2,2,2,2,2,2,2","Kg,Kg,Kg,Kg,Kg,Kg,Kg,Kg,Kg,Kg,Kg",axy2.0,0,4,""

ID,f.ID,FactorID,Name,TableName,Items,ItemNames,ItemDecimalPlaces,ItemUnits,f.Project,PlatformFactorId,FactorParent,FactorType
axy2.0:22367,axy2.0:189,189,车牌监测,T_THEMES_TRAFFIC_LOAD_SIMPLIFY,"GrossWeight,AxieWeight1,AxieWeight2,AxieWeight3,AxieWeight4,AxieWeight5,AxieWeight6,AxieWeight7,AxieWeight8,AxieWeight9,AxieWeight10","车重,轴1重,轴2重,轴3重,轴4重,轴5重,轴6重,轴7重,轴8重,轴9重,轴10重","2,2,2,2,2,2,2,2,2,2,2","Kg,Kg,Kg,Kg,Kg,Kg,Kg,Kg,Kg,Kg,Kg",axy2.0,0,4,""

ID,f.ID,FactorID,Name,TableName,Items,ItemNames,ItemDecimalPlaces,ItemUnits,f.Project,PlatformFactorId,FactorParent,FactorType
axy4_0:1261,axy4_0:7,7,车辆称重,车流量,"axleCount,axleLoad,carSpeed,direction,lane,license,load,strOverload,weightK","轴数,轴重,车速,方向,车道,车牌,车重,超载,车辆折算系数","3,3,3,3,3,3,3,3,3",",T,km/h,,,,T,,",axy4_0,0,2002,""

ID,f.ID,FactorID,Name,TableName,Items,ItemNames,ItemDecimalPlaces,ItemUnits,f.Project,PlatformFactorId,FactorParent,FactorType
axy4_0:38498,axy4_0:362,362,渣土车监测,渣土车监测,"direction,latitude,longitude,mileage,speed","方向,纬度,经度,里程,速度","3,3,3,3,3","°, , ,km,km/h",axy4_0,0,7012,""

ID,f.ID,FactorID,Name,TableName,Items,ItemNames,ItemDecimalPlaces,ItemUnits,f.Project,PlatformFactorId,FactorParent,FactorType
axy4_0:40642,axy4_0:7,7,车辆称重,车流量,"axleCount,axleLoad,carSpeed,direction,lane,license,load,strOverload,weightK","轴数,轴重,车速,方向,车道,车牌,车重,超载,车辆折算系数","3,3,3,3,3,3,3,3,3",",T,km/h,,,,T,,",axy4_0,0,2002,""
sampleFreq

ID,f.ID,FactorID,Name,TableName,Items,ItemNames,ItemDecimalPlaces,ItemUnits,f.Project,PlatformFactorId,FactorParent,FactorType
axy4_0:47690,axy4_0:421,421,车辆抓拍单元,抓拍单元,"color,deviceName,image,license,time","颜色,设备名称,抓拍图片,车牌,上报时间","3,3,3,3,3",",,,,",axy4_0,0,6013,""



{"CK":{"AT":"2025-05-29 13:58:59","GL":"115.00E_21.00N","CCID":"89860842032490022050","DID":"LWPTb5030006","HVER":"1.03","SVER":"2.191","BAT":100,"DB":-67,"ERR":2064,"CV":255,"CNT":1}}

{"ACK":"OK"}
2025/05/28 13:44:39.985029 [D] Service.OnStatus : mqtt:192.168.1.111:PLC - Closed
2025/05/28 13:44:39.985034 [D] [mqtt:192.168.1.111:PLC] Reconnecting...
2025/05/28 13:44:39.985040 [D] Service.OnStatus : mqtt:192.168.1.111:PLC - Closed
2025/05/28 13:44:39.985044 [D] [mqtt:192.168.1.111:PLC] Reconnecting...
2025/05/28 13:44:39.985049 [D] [mqtt:192.168.1.111:PLC] Reconnecting...
2025/05/28 13:44:39.985049 [D] Service.OnStatus : mqtt:192.168.1.111:PLC - Closed
2025/05/28 13:44:39.985056 [D] [mqtt:192.168.1.111:PLC] Reconnecting...
2025/05/28 13:44:39.985058 [D] Service.OnStatus : mqtt:192.168.1.111:PLC - Closed
2025/05/28 13:44:39.985063 [D] [mqtt:192.168.1.111:PLC] Reconnecting...
2025/05/28 13:44:39.985070 [D] [mqtt:192.168.1.111:PLC] Reconnecting...
2025/05/28 13:44:39.985071 [D] Service.OnStatus : mqtt:192.168.1.111:PLC - Closed
2025/05/28 13:44:37.759605 [E] [mqtt:192.168.1.111:PLC] read error: Mqtt unconnected
2025/05/28 13:44:39.985082 [D] [mqtt:192.168.1.111:PLC] Reconnecting...
2025/05/28 13:44:37.759602 [E] [mqtt:192.168.1.111:PLC] read error: Mqtt unconnected



2025/05/28 10:47:15.525223 [E] [mqtt:192.168.1.111:PLC] read error: Mqtt unconnected


2025/05/28 10:47:20.513422 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513508 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513519 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513652 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:22.634255 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513709 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513771 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513854 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513929 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513944 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.513984 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 
2025/05/28 10:47:20.514003 [D] [monitor] OnLink: [ThingID=051cf258-72d7-493e-ab70-d4c96fcc97e0] => 0 


发送shengguang告警失败，内容为：Map(param -> 1, thingId -> 03ee26c6-062a-4ae7-8b10-c41b45d739cb, dimCapId -> e5505a0f-db03-4076-9875-5a8e1b068e72, deviceId -> d85677de-ee0f-4325-a5f2-50d7a8c8ec1b, timeout -> 10000)，地址为：http://10.244.152.247:7007/_iota_api/capabilities/invoke


数据是固定周期上报的，从ES数据库中查询各个点位的数据，判断数据丢失的粒度。
python streamlit实现工具数据丢失情况查询：
输入结构物ID和起始+结束时间。
通过ES查询（已封装方法def query_es(index, query, token=""):
    url = f"http://218.3.126.49:19200/{index}/_search"
    # 设置查询的 header
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=query, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error querying Elasticsearch: {response.text}")
        return None）
        
查询数据的json：        
GET anxincloud_themes/_search
{
  "query": {
    "bool": {
      "must": [
        {"terms": {
          "structure": [
              3381
          ]
        }},
        {
          "range": {
            "collect_time": {
              "gte": "2025-05-26T00:00:00.000+0800",
              "lte": "2025-05-26T06:00:00.000+0800"
            }
          }
        }
      ]
    }
  },
  "sort": [
    {
      "create_time": {
        "order": "desc"
      }
    }
  ]
}
查询所有数据，其中查询结果中sensor字段标识传感器ID，collect_time是数据时间。
每个传感器依次判断，根据周期判断数据出现丢失的情况。

图表展示，展示每个点位（数据点数，理论点数，丢失点数），如果丢失点数>0，这一行为红色提醒。
图形展示，标注出丢失的时间点




golang实现一个工具，订阅kafka
输入一个设备：

用kafka bin工具调用查看数据，./kafka-console-consumer.sh --bootstrap-server iota-m2:6667 -topic RawData --from-beginning | grep 'e91633c0-0a11-4361-bfb0-40af04d99f88'  | grep '2025-05-26T05'
发现数据丢失了2条。 我们的进程也是订阅这个topic，只有这丢失的两条，这会是什么原因，kafka会分裂成多个集群吗？


./kafka-console-consumer.sh --bootstrap-server iota-m2:6667 -topic RawData --from-beginning | grep 'e91633c0-0a11-4361-bfb0-40af04d99f88'  | grep '2025-05-26T05'
./kafka-console-consumer.sh --bootstrap-server iota-n1:6667 -topic RawData --from-beginning | grep 'e91633c0-0a11-4361-bfb0-40af04d99f88'  | grep '2025-05-26T05'
./kafka-console-consumer.sh --bootstrap-server iota-n2:6667 -topic RawData --from-beginning | grep 'e91633c0-0a11-4361-bfb0-40af04d99f88'  | grep '2025-05-26T05'


scala代码拼接以下报文：
根据数据生成二进制。填充生成报文

协议说明		"数据包字段（16进制）
以下数据从左到右是从低字节到高字节"	解析结果
帧头		57 53	帧头
帧类型		01	数据帧
数据帧长度		3F 00	63字节
采集设备SN		E1 84 0D 01 E2 2A 02 00	610100122060001
时间戳		00 00 00 00 00 00 00 00	1970-01-01 08:00:00.000
数据项数量 		06	6
数据项1	传感器类型	44 2F	12100
	数值	E1 A4 5C 97 52 24 01 00	321412122060001 
数据项2	传感器类型	45 2F	12101
	数值 	71 3D 52 41	13.14
数据项3	传感器类型	47 2F	12103
	数值	00 00 C0 40	6
数据项4	传感器类型	44 2F	12100
	数值	E2 A4 5C 97 52 24 01 00	321412122060002 
数据项5	传感器类型	45 2F	12101
	数值 	71 3D 52 41	13.14
数据项6	传感器类型	47 2F	12103
	数值 	00 00 E0 40	7
滚动校验		C6 	198
校验和		72	0x72



systemctl enable sshd
55555

ethtool

vim /etc/sysconfig/network-scripts/ifcfgens8f0

BOOTPROTO=dhcp 改成BOOTPROTO=static
ONBOOT=no 改成 ONBOOT=yes

IPADDR=10.42.201.103 #静态IP 的配置
NETMASK=255.255.255.0 # 网关的配置上子网掩码
GATEWAY=10.42.201.254 # 网关的配置.


reload_NetworkManager
ip a

方法2：
root 登录后输入 nmtui




Scala实现：
请选择聚集方式
aggrMethod
  {value:'10min',label:'10min'},{value:'30min',label:'30min'},{value:'1h',label:'1h'},{value:'3h',label:'3h'},
  {value:'6h',label:'6h'},{value:'12h',label:'12h'},{value:'day',label:'天'},{value:'week',label:'周'},

请选择数据格式
dataFormat
 {value:'avg',label:'平均值'},{value:'max',label:'最大值'},{value:'min',label:'最小值'},{value:'median',label:'中值'},

实现ES查询方法，以下是参考，要修改支持所有聚集方式和数据格式，并同时聚集多个 itemName: String -> items:Array[String]。

    /**
      * 按照分钟聚集测点数据
      */
    def getStationDataAggMinutes(station: OStation, startTime: DateTime, endTime: DateTime, itemName: String, minutes: Int): Array[ThemeData] = {
        val query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("sensor", station.sid))
            .must(QueryBuilders.rangeQuery("collect_time").gte(startTime).lt(endTime))

        val isMed = false
        val aggregation = AggregationBuilders
            .dateHistogram("by_collect_time")
            .field("collect_time")
            .dateHistogramInterval(
                DateHistogramInterval.minutes(minutes))
            .timeZone(DateTimeZone.forID("Asia/Shanghai"))
            .format("yyyy-MM-dd HH:mm:ss")
            .subAggregation(AggregationBuilders.avg(itemName).field(s"data.$itemName"))


        val aggregations = ESHelper.queryAgg(ESHelper.themeIndex, ESHelper.themeType, query, aggregation)
        val dateHistogram = aggregations.get("by_collect_time").asInstanceOf[Histogram]
        val dateHist = dateHistogram.getBuckets.asScala.map(histBucket => {
            val date = DateParser.parse(histBucket.getKeyAsString)
            val data = histBucket.getAggregations.asMap().asScala.toMap.map(d =>
                (d._1, if (!isMed) {
                    convertNaN(d._2.asInstanceOf[ParsedSingleValueNumericMetricsAggregation].value())
                } else {
                    convertNaN(d._2.asInstanceOf[ParsedTDigestPercentiles].percentile(50))
                })
            ).filter(d => !d._2.isNaN)
            ThemeData(date.get, data)
        }).filter(d => d.data.nonEmpty).toArray

        dateHist
    }




实现AI滑坡灾害识别功能，将海康摄像头接入平台，服务器上部署海康ISC平台、云端抓图解算。并将数据结果通过MQTT发布：
mqtt.datahub.anxinyun.cn
发布的topic中包含摄像头的序列号：如20250412CCCHFY8123364
发布内容参考：
字段	是否必填	类型	字段含义
deviceId	是	string	报警设备ID
alarmTime	是	long	报警时间 （精确到秒的时间戳）
alarmType	否	string	报警类型
alarmMsg	是	string	报警描述
photoData	是	string	照片文件的base64数据

请帮忙编写云端解算告警数据格式.md

location / {
        proxy_pass    http://192.168.0.2:8000;
        proxy_cookie_domain localhost localhost;
        proxy_cookie_path / /;
        proxy_connect_timeout 1;
        proxy_send_timeout 600;
        proxy_read_timeout 60;
        proxy_buffering off;
    }
golang实现一个客户端+服务端websocket通信程序。
客户端：
连接，发送唯一标识ProjectId（string）。

服务端：
接收存入到Map[Project,Conn]。
服务端方法： QueryRemote(project,Request) 找到conn发送请求，同步等待响应Response。

（Request和Response是接口，设计参考HTTP包括方法、参数，通过Protobuf进行数据传输）

实现一个场景，继承Request/Response的特殊实现：请求指定id的传感器设备数据的查询：
type SensorDataRequest struct {
	SensorId string
	Start    time.Time
	End      time.Time
}

type SensorDataResponse struct {
	Datas []struct {
		Time   time.Time
		Values []float64
	}
}


用scalaj.http实现上报：
参数名称	类型	必须	描述	示例值
BridgeCode	string	是	桥梁编码	略
File	文件	是	文件,文件的文件名称确保唯一，不能重复调用	略
请求ContentType
multipart/form-data

隆江大桥

桥底水位	xxx  {"Data":"水位"}    地下水位,1005  waterLevel
主梁纵向振动加速度	/RealtimeData/A-25	{"Data":"加速度数据"}   桥面振动,5002
主梁竖向振动加速度	
桥墩顶部纵向振动加速度	
桥墩转角	/RealtimeData/A-20	{"X":"横桥向","Y":"纵桥向","Temp":"温度"}   桥墩倾斜,4007 (x,y)
梁端水平转角	/RealtimeData/A-20	{"X":"横桥向","Y":"纵桥向","Temp":"温度"}

主梁竖向位移	/RealtimeData/A-17	{"Data":"位移","Temp":"温度"}    支护结构变形,4002 (x,y)
混凝土结构裂缝	/RealtimeData/A-27	{"Data":"裂缝值","Temp":"温度"}    裂缝监测,4008   crack
主梁关键截面应变	/RealtimeData/A-21	{"Data":"应变值","Temp":"温度"}   应变,3001   strain

"(光纤通断监测,1070)"
"(地下水位,1005)"
"(应变,3001)"
"(支护结构变形,4002)"
"(桥墩倾斜,4007)"
"(桥面振动,5002)"
"(梁体振动,5002)"
"(裂缝监测,4008)"



golang实现一个系统。客户端是远程工控机，启动后就tcp（或其他方式）连接到服务器，保持长连接。服务器上可以根据这个socket，去请求工控机客户端上的服务。
定义一个协议：
{
"":"",
}




第4章	项目整合管理	学习到项目整合管理 (Project Integration Management)的一些知识，对项目整合有一定的理解
第5章	项目范围管理	在软件研发工作中，项目范围管理和范围蔓延是比较容易出现的问题。通过本章节的学习，我理解到工作必须与实际的需求完全契合，不能少也不能多。确保任务分配符合项目范围，避免范围蔓延。
第6章	项目进度管理	通过制定和维护项目进度表，评估和调整团队工时。主要工作中，配合项目经理，对工期中主要工作节点工时进行评估确认，对关键进度节点进行确认。尤其是相互依赖的任务中，必须协调好处理的优先顺序。
第7章	项目成本管理	同上，作为组长还是主要负责协同项目经理，做好工时评估与成本控制，确保项目在预算范围内完成
第8章	项目质量管理	确保任务和交付物符合质量标准，定期质量评审。对质量提高重视，理解测试工作的紧要性，同时会组织复盘，并对经验成果进行总结和分享。
第9章	项目资源管理	有效分配和管理团队资源，提升团队效率。主要是工作人力分配中，理解和应用资源获取计划，对组内成员工作安排有详细的了解。确保团队成员之间的有效沟通，以及与利益相关者之间的良好沟通。
第10章	项目沟通管理	每日站会组织，定期汇报项目进展，保持团队内部和外部的沟通顺畅。同时不断学习与不通成员之间的沟通表达技巧，确保任务需求的完整准确传递。

,
        {
          "name": "含水率",
          "filter": {
            "proto": "4015"
          },
          "route": "/Api/V1/SlopeData/UploadJYL",
          "body": "{\"code\":\"${deviceCode}\",\"structureCode\":\"${structureCode}\",\"dataTime\":\"${time}\",\"data\":[{\"data\":${data.moiscont100}}]}"
        }

海康摄像头设置了联动方式，如何通过golang程序接收它的告警事件。

海康 应用仓库 AI开放平台配置

{\"code\":\"${deviceCode}\",\"structureCode\":\"${structureCode}\",\"dataTime\":\"${time}\",\"data\":[{\"x\":${x},\"y\":${y},\"displacement\":${displacement},\"depth\":${depth}}]}

项目：中山中开高速火炬服务区边坡监测项目
1.数据推送需求
需要推送的测点：GNSS-监测站K8-BD02、GNSS-监测站K8-BD01、土壤含水率、 测斜监测K8-CX01-1~测斜监测K8-CX01-15
完成时间节点：本周内。
第三方平台：广东省长大桥梁健康监测系统开发者接入平台(https://bmsapi.gdjkjc.cn:13579)
开发文档地址：https://dev.gdjkjc.cn:50001/ 请看首页接入流程。[账号yangguofei 密码yangguofei123456]
接口地址：https://bmsapi.gdjkjc.cn:13579
2.单结构物系统需要提供单点登录对接
3.提供一个post获取token的接口   
rnd 10位的时间戳
code 不定结构物全国统一编号        G228440781L1640
sign 采用加密算法 sign = Sha1(appKey + code + rnd + appSecret)
4.提供一个通过token跳转到系统的接口
@许苏炜 

golang实现数据入库influxdb2
influx setup --username fs --password 123456 --org free-sun --bucket data --token vUoB3dCDNkdo26uoBtpVACRjLj71REE8IgXBwYZI3n9D5Ts2wTEaCGc2OWYPq0cadwEiHEM3caJz5Jm8IVGFBQ== --force

influx org create --name free-sun
influx auth create --org free-sun --all-access
influx bucket create --name data --org free-sun --retention 30d

Remove-Item -Recurse -Force "$env:USERPROFILE\.influxdbv2"


// 解析后的数据将用于后续处理，具体包括传递给数据存储模块进行持久化保存，以及传递给数据推送模块实现实时分发。
type ParsedData struct {
	// 上报平台设备标识
	DeviceID   string `json:"id"`
	DeviceName string `json:"deviceName"`
	Factor     string `json:"factor"` // 监测因素/传感器类型
	// 本系统设备标识
	Module    string      `json:"module"`
	Timestamp time.Time   `json:"time"`
	Value     [][]float32 `json:"value"` // 采集样本列表，如： [[x1,y1],[x2,y2],[x3,y3],...]
}
Value数组长度大于1，则打散第一个时间为Timestamp，后面每个加1/长度的秒。


// InfluxDBOutput 实现了 DataOutput 接口
type InfluxDBOutput struct {
	client        influxdb2.Client
	api           api.WriteAPI
	qapi          api.QueryAPI
	org           string
	bucket        string
	retryLimits   int
	wg            sync.WaitGroup
	errChan       chan error
	batchSize     int
	flushInterval time.Duration

	batches     map[string][]*write.Point // 按 bucket 分组的批量数据
	batchesLock sync.Mutex
	stopChan    chan struct{}
}

// NewInfluxDBOutput 创建新的 InfluxDB 输出实例
func NewInfluxDBOutput(url, token, org string, batchSize int, flushInterval time.Duration) (*InfluxDBOutput, error) {
	client := influxdb2.NewClient(url, token)
	// 测试连接是否有效
	_, err := client.Health(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to InfluxDB: %v", err)
	}

	output := &InfluxDBOutput{
		client:        client,
		org:           org,
		errChan:       make(chan error, 100),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		batches:       make(map[string][]*write.Point),
		stopChan:      make(chan struct{}),
		retryLimits:   3,
	}

	output.api = output.client.WriteAPI(output.org, output.bucket)
	output.api.SetWriteFailedCallback(output.writeFailedCallback)
	output.qapi = output.client.QueryAPI(output.org)

	return output, nil
}

func (db *InfluxDBOutput) writeFailedCallback(_ string, error http2.Error, retryAttempts uint) bool {
	if int(retryAttempts) < db.retryLimits {
		return true
	} else {
		// discard
		utils.Warn("[influx] write discard after %d attempts: %v", retryAttempts, error)
		return false
	}
}

func (db *InfluxDBOutput) saveByPoints(tb string, arr device.ParsedData) error {
	defer func() {
		if r := recover(); r != nil {
			utils.Warn("save points panic: %v", r)
		}
	}()
}





[2025-05-12 13:26:03,143] [urllib3.connectionpool::urlopen::827] [WARNING] Retrying (Retry(total=0, connect=None, read=None, redirect=None, status=None)) after connection
broken by 'SSLError(SSLEOFError(8, 'EOF occurred in violation of protocol (_ssl.c:997)'))': /api/5820521/envelope/
FS-Q90-NHB1-V1.00
micro-power

def sendTo(sendTo: Seq[String], content: String, subject: String, params: Map[String, Any]): Boolean = try { println(s"发送参数: 收件人=${sendTo.mkString(",")}, 主题=$subject, SMTP=${params("email_host")}:${params("email_smtp_port")}, 发件人=${params("email_from")}(${params("email_from_name")})"); val email = new HtmlEmail().setHostName(params("email_host").asInstanceOf[String]).setSmtpPort(params("email_smtp_port").asInstanceOf[Int]).setAuthentication(params("email_from").asInstanceOf[String], params("email_from_pwd").asInstanceOf[String]).setSSLOnConnect(true).setCharset("gbk").setFrom(params("email_from").asInstanceOf[String], params("email_from_name").asInstanceOf[String]); sendTo.foreach(email.addTo); email.setSubject(subject).setHtmlMsg(content).send(); info(s"邮件发送成功: $sendTo $subject"); true } catch { case ex: Exception => warn(s"邮件发送失败: to=$sendTo subject=$subject", ex); false }

开发一个winform程序，从ck数据库获取海量数据。考虑： 使用lightingChart作为展示组件（10.5.1）。ck分页查询分段加入chart中。ck查询数据提高速度（sql传输内容是文本吗，可以使用protobuf这种传输提升效率吗）。

需要增加配置：
alarm.msg.template.sms=您好，您收到一条信息，${req.alarm.extras.structureName}于${req.alarm.endTime:yyyy-MM-dd HH:mm}产生告警，告警内容为: ${req.alarm.alarmContent}${detail_content}，请及时关注信息内容，注意防范。
templateCode=SMS_485450111
templateName=中冶建研院深
ali.accessKeyId=xxx
ali.accessKeySecret=xxx

发发发：
监控DAC的码流、协议日志。



EXCEL中数据：
name	serial_no	channel_no	project_name
小三岔口二号中桥	33010820992737659798:33011010991327585334	1	承德桥梁安全监测平台
机场路连接线特大桥（下行）	33010820992737659798:33010463991327109562	1	承德桥梁安全监测平台

编写excel字符串拼接，生成如下
insert into camera ( type, name, channel_name, external_domain, rtmp, serial_no, cloud_control, high_definition, voice, memory_card, vender_id, cascade_type, sip, longitude, latitude, forbidden, create_time, recycle_time, delete, create_user_id, nvr_id, model, kind_id, yingshi_secret_id, gb_id, top_serial_no, channel_no, definition)
values ('yingshi', '	小三岔口二号中桥	, NULL, NULL, '此处无效 自动生成', '	33010820992737659798:33011010991327585334'	, '1', '1', NULL, NULL, NULL, NULL, NULL, 117.440000000000, 40.340000000000, '0', '2025-04-23 18:13:50+08', NULL, '0', 2149, NULL, NULL, 2, 1, 1975,
        NULL, '1', '1', 'ON');
        
=CONCATENATE(
"('yingshi', '",
A2,
"', NULL, NULL, '此处无效 自动生成', '",
B2,
"', true, true, true, NULL, NULL, NULL, NULL, 117.440000000000, 40.340000000000, false, '2025-04-23 18:13:50+08', NULL, false, 2149, NULL, NULL, 2, 1, 1975, NULL, '",
C2,
"', true, 'ON'),"
)


scala
    // 通过.操作符获取子属性
    def getSubField(v: Any, fieldPath: String): Any = {
        if (fieldPath == null || fieldPath.length == 0) v
        else {
            val sps = fieldPath.split("\\.", 2).map(_.trim)
            val subv =
                v match {
                    case mp: Map[String, Any] =>
                        mp.getOrElse(sps(0), "")
                    case _ =>
                        val fieldOpt = v.getClass.getDeclaredFields.find(_.getName.compareToIgnoreCase(sps(0)) == 0)
                        if (fieldOpt.nonEmpty) {
                            fieldOpt.get.setAccessible(true)
                            fieldOpt.get.get(v)
                        } else {
                            ""
                        }
                }

            if (sps.length > 1)
                getSubField(subv, sps(1))
            else
                subv
        }
    }   
遇到Option类型的时候，取出里面的数据 
    

我有个pg数据库，如何在做一个备份。
备份数据库可以读取吗，不需要实时同步性多高。

输入任意设备的码流报文给大模型，让大模型给我识别出其中的数据。怎么设计和开发

{"model":"recalc","stationId":[54450],"msgId":"cf768a6e-d46c-4b11-ad1c-192f5251b900","dataBeginTime":"2025-04-04T03:01:11.000Z","dataEndTime":"2025-05-06T03:01:11.000Z","operation":"add"}
数据下载\n数据查询
                imageUrl: "http://resources.anxinyun.cn/tmpj323bd49logo1.jpg",


实现边坡滑坡预警AI识别功能，现在有两种方案：
1. 通过海康摄像头+海康威视AI开放平台（包括实时训练和算法应用平台） 实现滑坡算法训练和模型下发到摄像头，在设备上产生告警信息
2. 标准摄像头+萤石云+实时图片流调用大模型直接识别

请结合输出这两种方案的完整内容。
并画出两种方案的流程图


通过萤石云接口，获取设备当最新图片，调用大模型接口，判断是否有滑坡。
1. 获取token。（并定时每天更新）
https://open.ys7.com/api/lapp/token/get

2. 设备抓拍图片
https://open.ys7.com/api/lapp/device/capture

参数名	类型	描述	是否必选
accessToken	String	授权过程获取的access_token	Y
deviceSerial	String	设备序列号,存在英文字母的设备序列号，字母需为大写	Y
channelNo	int	通道号，IPC设备填写1	Y
quality	int	视频清晰度,0-流畅,1-高清(720P),2-4CIF,3-1080P,4-400w	N

3. 调用大模型接口判断是否滑坡



deviceSerial=FQ6615540&channelNo=1&accessType=1&playType=1


python-streamlit开发一个系统服务器配置计算工具。
先做磁盘容量估算的工具，需要录入：
数据采集粒度
    1. 普通监测点位个数 * 监测粒度
    2. 高频数据点位个数 * 监测粒度
存储方式：
    1. ElasticSearch存储（并输入副本数）
    2. InfluxDB存储
部署方式：
    独立服务器
    服务器集群（个数）

如：
给到下面清单：
设备	数量	hz
测地型GNSS接收机 	7	1/3600
拉线位移传感器	16	1
加速度计	216	50
三向加速度计	4	50
光纤光栅应变计（带温补）	90	10
光电挠度仪	76（每台2个标靶）	20
索力测量雷达	2	10
光波一体机	2	20
应变计	200	1/600
温湿度传感器	16	1/600
静力水准仪	43	1
激光测距仪	28	1/600
风速风向仪（三向）	2	10
		
其中ES中存储的字段包括：
"properties" : {
          "batchid" : {
            "type" : "keyword"
          },
          "collect_time" : {
            "type" : "date",
            "format" : "date_time || yyy-MM-dd HH:mm:ss"
          },
          "create_time" : {
            "type" : "date",
            "format" : "date_time || yyyy-MM-dd HH:mm:ss"
          },
          "factor" : {
            "type" : "integer"
          },
          "factor_proto_code" : {
            "type" : "keyword"
          },
          "iota_device" : {
            "type" : "keyword"
          },
          "sensor" : {
            "type" : "integer"
          },
          "sensor_name" : {
            "type" : "text",
            "norms" : false,
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 50
              }
            }
          },
          "structure" : {
            "type" : "long"
          },
          "data" : {
            "dynamic" : "true",
            "properties" : {
              "temperature" : {
                "type" : "float"
              },
              ...
            }
          }
        }
         
InfluxDB存储中tag就是int类型的sensorid，数据是浮点型。

无论是influxdb还是es，数据的个数跟传感器的类型相关。如应变传感器，只输出应变值一个浮点数。而温湿度传感器，包括温度、湿度两个浮点型数据的输出。

golang做一个定时监控工具。如果发现Airbyte数据同步失败，发送邮件给我。

POST https://airbytes.ngaiot.com/api/v1/web_backend/connections/list
BODY:{"workspaceId":"6547548c-1778-4ad9-ad4a-0ef5c54847df"}
HEADER:authorization: Basic YWlyYnl0ZTpGczEyMw==
返回：
{
  "connections": [
    {
      "connectionId": "9aab8e42-0f84-4423-a5bd-15aebc1a2cac",
      "name": "PG-Poms → CK-Poms",
      "scheduleType": "basic",
      "scheduleData": {
        "basicSchedule": {
          "timeUnit": "hours",
          "units": 2
        }
      },
      "status": "active",
      "source": {
        "sourceId": "a62d386d-23f5-41b1-9e24-b9375a43bf16",
        "name": "PG-Poms",
        "sourceDefinitionId": "decd338e-5647-4c0b-adf4-da0e75f5a750",
        "sourceName": "Postgres"
      },
      "destination": {
        "destinationId": "474a95cb-941f-4fd3-927d-a229a55496d8",
        "name": "CK-Poms",
        "destinationDefinitionId": "ce0d828e-1dc4-496c-b122-2da42e637e48",
        "destinationName": "Clickhouse"
      },
      "latestSyncJobCreatedAt": 1746572332,
      "latestSyncJobStatus": "succeeded",
      "isSyncing": false,
      "schemaChange": "non_breaking"
    }
  ]
}

只判断"status": "active"的连接。
判断"latestSyncJobStatus": "running"/"succeeded"/"失败"， 如果是失败则报警。
只报警latestSyncJobCreatedAt在一个小时内的错误。 并且在程序中记录报过警的latestSyncJobCreatedAt，不重复报。
