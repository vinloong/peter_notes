root@ok335x:/fs# netstat -ant
Active Internet connections (servers and established)
Proto Recv-Q Send-Q Local Address           Foreign Address         State
tcp        0      0 0.0.0.0:21              0.0.0.0:*               LISTEN
tcp        0      0 192.168.1.13:52681      192.168.1.108:8888      ESTABLISHED
tcp        0      0 :::23                   :::*                    LISTEN
tcp        0    303 ::ffff:192.168.1.13:23  ::ffff:192.168.1.108:52356 ESTABLISHED

如果发现其中连接到192.168.1.108:8888的连接，就重启该进程。修改以下脚本：

#!/bin/bash

# 注：本脚本需要以超级用户身份运行。

INTERVAL=10
PROGRAME=/fs/lux
THREADNUMS=1
NO_CONN_COUNT=0         # 无连接计数器
MAX_NO_CONN=10          # 最大无连接次数

echo "***********************Watch Dog Begin***********************"
sleep 10

while true
do
    SYSDATE=$(date)

    # 检查 /fs/lux 是否存在
    nowps1=$(ps | grep -w $PROGRAME | grep -v grep | wc -l)

    # 检查是否有连接到 192.168.1.108:8888
    conn_exists=$(netstat -ant | grep '192.168.1.108:8888' | grep ESTABLISHED | wc -l)

    if [ $conn_exists -eq 0 ]; then
        NO_CONN_COUNT=$((NO_CONN_COUNT + 1))
    else
        NO_CONN_COUNT=0
    fi

    # 如果线程数量不对 或者 已连续10次无连接
    if [ $nowps1 -ne $THREADNUMS ] || [ $NO_CONN_COUNT -ge $MAX_NO_CONN ]; then
        echo "*******************************************************"
        echo "/fs/lux is abnormal or no connection to 192.168.1.108:8888 for $NO_CONN_COUNT times! time: $SYSDATE"
        echo "------------------ /fs/lux need restart -------------------"
        echo "*******************************************************"

        # 杀掉旧进程
        ps | grep $PROGRAME | grep -v grep | awk '{print $1}' | xargs kill -9

        # 重启进程
        $PROGRAME &

        NO_CONN_COUNT=0
        sleep 5
    fi

    sleep $INTERVAL
done

echo "***********************Watch Dog End***********************"
exit 0
