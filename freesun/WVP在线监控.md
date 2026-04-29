Golang开发一个wvp视频在线监控工具。
定时执行（用cron。每小时）
需要监控哪些设备，从PG数据库中查询。【设备ID（由设备编号和通道编号组成，如34020000001110000081/34020000001310000001）】-【名称】
然后，依次调用WVP登录、请求播放、zlm截图接口获取指定摄像头最新图片。
使用yaml配置（包括pg数据库地址、用户名密码、WVP平台地址、用户密码）

1. 登录
GET http://14.103.160.127:32188/api/user/login?username=admin&password=21232f297a57a5a743894a0e4a801fc3

返回
{
    "code": 0,
    "data": {
        "accessToken": "eyJhbGciOiJSUzI1NiIsImtpZCI6IjNlNzk2NDZjNGRiYzQwODM4M2E5ZWVkMDlmMmI4NWFlIn0.eyJqdGkiOiI3OUtzdHhWVE8zMXRDclNZUFBpNWlRIiwiaWF0IjoxNzQ1OTI2ODUwLCJleHAiOjE3NDU5Mjg2NTAsIm5iZiI6MTc0NTkyNjg1MCwic3ViIjoibG9naW4iLCJhdWQiOiJBdWRpZW5jZSIsInVzZXJuYW1lIjoiYWRtaW4iLCJwYXNzd29yZCI6IjIxMjMyZjI5N2E1N2E1YTc0Mzg5NGEwZTRhODAxZmMzIiwicm9sZUlkIjoxfQ.beKDowshH6if8_pBfovcJsqlUcnRskDOQNvjNShRNlE0FdI-Bvy8vHTytk0psoxN7NmHNUPSBN9ItvLpQ0kx_qF3oFciEWXdz1QxOqjL7wF8oxKWFW__fjI0rh7yMT2xZcPd9TPlo-8hLESIPVbJAMeZrE-13iMUJwHdTw5d_1Ir01MPFtOuvLOuNJsn67FE9IGZ6f_F5Q2xJjaJQnQRJmM0YfuoFrmbDkyw6_sxAXeWeNEQHGoKD4Nw9O1jUMYTNoNz8uBKvJN7ZrJ94NE4LLwSIPjTaRz2BZAIpL8rdA32mRwvzsDgUYxzkItBO1YJhMhbVaWo7ir8g9CV_ZQ84A",
        "accountNonExpired": true
    }
}
放在头access-token中请求以下接口。

2. 播放
GET http://14.103.160.127:32188/api/play/start/34020000001110000081/34020000001310000001?isSubStream=false
返回：
{
    "code": 0,
    "data": {
        "app": "rtp",
        "flv": "http://14.103.160.127:18080/rtp/05F5EB91.live.flv",
        "fmp4": "http://14.103.160.127:18080/rtp/05F5EB91.live.mp4",
        "hls": "http://14.103.160.127:18080/rtp/05F5EB91/hls.m3u8",
        "https_flv": "https://14.103.160.127:18080/rtp/05F5EB91.live.flv",
        "https_fmp4": "https://14.103.160.127:18080/rtp/05F5EB91.live.mp4",
        "https_hls": "https://14.103.160.127:18080/rtp/05F5EB91/hls.m3u8",
        "https_ts": "https://14.103.160.127:18080/rtp/05F5EB91.live.ts",
        "mediaServerId": "D42p1dNGHH6jyleL",
        "progress": 0.0,
        "rtc": "http://14.103.160.127:18080/index/api/webrtc?app=rtp&stream=05F5EB91&type=play",
        "rtcs": "https://14.103.160.127:18080/index/api/webrtc?app=rtp&stream=05F5EB91&type=play",
        "rtmp": "rtmp://14.103.160.127:1935/rtp/05F5EB91",
        "rtmps": "rtmps://14.103.160.127:19350/rtp/05F5EB91",
        "rtsp": "rtsp://14.103.160.127:1554/rtp/05F5EB91",
        "rtsps": "rtsps://14.103.160.127:1332/rtp/05F5EB91",
        "stream": "05F5EB91"
    }
}

3.截图
调用外部ffmpeg程序截图。
root@iv-ydtrju48w0wuxjsooe7j:~/wvp# ffmpeg  -i rtmp://192.168.0.2:11935/rtp/05F5F703 -y -f mjpeg -t 0.001 /tmp/1.jpeg



4.将结果保存到CK
结果存储到PG数据库。
保存：【设备ID】-【检查时间】-【耗时】-【信令在线】-【播放状态】-【截图状态】-【截图】

并通过prometheus记录状态【信令在线】【播放状态】【截图状态】



> ZLM自带的截屏不太好用，提示transport_tcp ffmpeg的option不存在。
http://<ZLMediaKit服务器IP>:<端口>/index/api/getSnap?url=rtsp://<设备流地址>&timeout_sec=5&expire_sec=10
curl "http://192.168.0.2:18080/index/api/getSnap?url=rtmp://192.168.0.2:1554/rtp/05F5F942&timeout_sec=10&expire_sec=30&secret=tXQA3PJL6RNnH8kEwAG2ETs25VyVzrJn" --output 1.png

curl "http://192.168.0.2:18080/index/api/getSnap?url=http://192.168.0.2:18080/rtp/05F5F942.live.flv&timeout_sec=10&expire_sec=30&secret=tXQA3PJL6RNnH8kEwAG2ETs25VyVzrJn" --output 1.png