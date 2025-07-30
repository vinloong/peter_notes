# 海康摄像头抓拍识别功能说明

## 1、 项目概述

该系统基于海康 ISC 平台，实现摄像头图像定时抓拍并调用大模型进行灾害识别分析，支持通过 MQTT 实时发布告警消息及事件详情，并将结果存入数据库。

---

## 2、 项目结构

* **代码路径**：`FS-IOT/trunk/code/gowork/src/video-recognition`
* **构建地址**：[Jenkins 构建](https://jenkins.ngaiot.com/view/01%20-%20IOT/job/video-recognition/)
* **服务发布**：[部署状态](https://ksboard.ngaiot.com/cloud-workspace/clusters/default/projects/iot/deployments/video-recognition/resource-status)
* **临时发布地址**： http://222.186.227.196:32145/
* **数据库**：210服务器，数据库名：`aicracker`

---

## 3、 核心功能

### 1. 摄像头配置管理（UI界面）

* 支持配置海康与萤石摄像头设备

* 可设置：

    * **设备类型** （海康ISC或萤石云摄像头）
    * **目标摄像头** 选择ISC摄像头或者萤石摄像头编号
    * **名称** 定义任务的名称
    * **识别周期（单位秒）**
    * **启用状态**
    * **识别模型类型（如滑坡识别）**
    * **自定义提示词（可选）**

* 配置后，系统将根据周期定时抓拍图像并调用 AI 模型处理。

---

### 2. 抓拍与识别

* 系统通过海康 ISC 接口抓拍图像。
* 抓拍周期由前端配置指定。
* 调用大模型接口识别图像内容（如滑坡、坍塌、火灾等灾害事件）。

---

### 3. 消息发布（MQTT）

#### 3.1 报警消息

​	在摄像头识别到AI告警之后，向MQTT发布本告警消息：

- **MQTT 服务器地址**：`mqtt.datahub.anxinyun.cn`

- **协议端口**：默认 `1883`（无加密）

- **`QoS`**: 1或2

- **消息保留**：启用

- **发布主题格式（Topic）**：

  ```php+HTML
  ai/alarm/<摄像头序列号>
  ```

  示例：

  ```php+HTML
  ai/alarm/20250412CCCHFY8123364
  ```

消息采用 JSON 格式，字段说明如下：

##### JSON 数据结构

```json
{
  "deviceId": "20250412CCCHFY8123364",
  "alarmTime": 1716189023,
  "alarmType": "landslide",
  "photoPath": "图片URL"
}
```

##### 字段说明

| 字段名      | 类型   | 是否必填 | 含义                         | 示例值                                     |
| ----------- | ------ | -------- | ---------------------------- | ------------------------------------------ |
| `deviceId`  | string | 是       | 摄像头设备序列号或ID         | `20250412CCCHFY8123364`                    |
| `alarmTime` | long   | 是       | 报警时间（Unix时间戳，秒）   | `1716189023`                               |
| `alarmType` | string | 否       | 报警类型（如滑坡/滚石/崩塌） | `landslide`、rolling-stone、pothole        |
| `photoPath` | string | 是       | 报警截图的URL地址            | `“{网关ID}/yyyy/{yyyyMMddHHmmss}{No}.jpg"` |



#### 3.2 识别事件

* Topic: `ai/recognize/{camera_id}`
* 内容结构：

```json
{
  "photoPath": "图片URL",
  "photoData": "Base64图像",
  "detailMsg": "中文识别说明",
  "confidence": 置信度(0-100),
  "level": 事件等级,
  "location": {
    "lat": 纬度,
    "lon": 经度
  },
  "deviceId": "设备编号",
  "alarmTime": 识别时间戳,
  "alarmType": "事件类型", // 无告警时显示none.否则显示识别对应内容 landslide | collapse | fire等
  "alarmMsg": "英文警报提示"
}
```

---

## 4、 外部系统对接说明

* **海康ISC平台**：
* 地址：[https://221.230.55.29:10443/portal/](https://221.230.55.29:10443/portal/)
    * 账号：`admin`
    * 密码：`F**2025_`

---

## 5、 示例配置记录（前端展示）

| ID | 类型      | 名称    | 序列号                              | 点位     | 周期 | 模型类型      | 状态 | 创建时间                |
| -- | ------- | ----- | -------------------------------- | ------ | -- | --------- | -- | ------------------- |
| 1  | yingshi | 测试1   | FQ6615540                        | -      | 30 | landslide | 启用 | 2025-06-03 17:12:38 |
| 3  | hikisc  | 测试Hik | 451bdcca02884e98859820ddda686ada | IPdome | 10 | landslide | 启用 | 2025-06-03 22:38:16 |

---



