## DAAS计算输出格式说明

计算数据结果以MQTT JSON消息格式推送。

 JSON 数据由 计算结果`CableData` 类序列化生成，主要用于传输振动类传感器的数据，包含传感器编号、通道、频率、测量值（索力）、时间戳等信息。

------

## 🔧 数据结构定义

```json
{
  "Sensorid": 123, // 测点唯一编号（传感器配置清单XML文件中指定）
  "Module": "6666", // 模块号
  "Channel": 1, // 通道
  "Frequency": 200, // 采集频率
  "ThemeValue": [1234.0],  // 索力
  "RawValues": [], // 暂无
  "AccValues": [0.123, 0.456, 0.789 ...], // 原始波形数据
  "Ticks": 1721283012345 // 时间戳
}
```



## 📌 字段说明

| 字段名       | 类型       | 说明                                                         |
| ------------ | ---------- | ------------------------------------------------------------ |
| `Sensorid`   | `int`      | 传感器编号，对应 `sen.SensorID`。                            |
| `Module`     | `string`   | 模块号/测点号，上例中模块号6666                              |
| `Channel`    | `int`      | 通道号                                                       |
| `Frequency`  | `int`      | 采样频率（单位：Hz）                                         |
| `ThemeValue` | `double[]` | 主题值，可用于主通道分析值，如索力值、加速度 RMS、峰值等     |
| `RawValues`  | `double[]` | 原始数据，目前为空数组（`[]`）                               |
| `AccValues`  | `double[]` | 加速度时域波形数据，保留三位小数                             |
| `Ticks`      | `long`     | 时间戳（毫秒级，Unix 时间戳，长度 13 位），表示当前 UTC 时间（1970-01-01 起） |



## 配置说明

`app.config` 文件指定`mqtt`服务器地址：

```xml
<!-- Mqtt Server IP -->
    <add key="MQTT_BROKER_HOST" value="localhost"/>
    <!--Mqtt 端口 默认 1883-->
    <add key="MQTT_BROKER_PORT" value="1883"/>
    <!--Mqtt Client ID-->
    <add key="MQTT_CLIENT_ID" value="testid"/>
    <!--Mqtt 用户名-->
    <add key="MQTT_USER" value="cveCU7HPjq"/>
    <!--Mqtt 密码-->
    <add key="MQTT_PASSWD" value="kxqpmB7GFajo7f3uO+oTy3mke6B9uq3YdBaOZcbNjw4="/>
```

 `SenParamsConfigs.xml` 传感器计算公式清单

```xml
	<!--加速度监测-->
	<sensor id="47388001" module="Test" channel="1" >
        <!--灵敏度系数-->
		<formula id="26" sensitivity="23"/>
	</sensor>

	<!--索力监测-->
	<sensor id="47388004" module="47388" channel="4" >
        <!--灵敏度系数、索长、线密度、基频估算、基频误差、谐波阶数-->
		<formula id="47" sensitivity="1.0" l="91.647" m="115.2011522" f1="9.735" δ="0.325" k="1"/>
	</sensor>
```

