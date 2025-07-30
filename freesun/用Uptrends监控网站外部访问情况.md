用Uptrends监控网站外部访问情况

https://app.uptrends.com/ValueHub



![image-20250723151820965](imgs/用Uptrends监控网站外部访问情况/image-20250723151820965.png)



zhiwucloud-Browers check

https://app.uptrends.com/Report/ProbeDashboardFpc?probeGuids=41fc9761-3e59-45c1-9170-40cbf6128f44



zhiwucloud -Uptime

https://app.uptrends.com/Report/ProbeDashboardGeneric?probeGuids=0447bf48-65f2-45b1-801d-7d33b2e1735c#



| 字段名               | 含义                                        |
| -------------------- | ------------------------------------------- |
| **Monitor**          | 检查类型（如 Uptime、SSL 证书、浏览器检查） |
| **Uptime (%)**       | 可用率：在检测周期内网站正常工作的百分比    |
| **Checks**           | 执行了多少次检查                            |
| **Confirmed errors** | 检测到并确认的问题次数（不是暂时性错误）    |

检查域名能不能正常Ping通(是否网络可达)

https://ping.pe/zhiwucloud.com

![image-20250723153918750](imgs/用Uptrends监控网站外部访问情况/image-20250723153918750.png)

SSL检查：https://www.ssllabs.com/ssltest/analyze.html?d=www.zhiwucloud.com

![image-20250723154046741](imgs/用Uptrends监控网站外部访问情况/image-20250723154046741.png)