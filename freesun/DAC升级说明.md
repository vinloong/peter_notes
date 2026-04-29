#### DAC升级说明

构建

https://jenkins.ngaiot.com/job/iot-dac/



到anxin-m1服务器,升级版本号

```sh
root@anxin-m1:/home/anxin/iota/orchestrator# vi iota-pod-template.yml
# 修改对应版本号
root@anxin-m1:/home/anxin/iota/orchestrator# kubectl apply -f iota-pod-template.yml

```



镜像分发到anxin-m2

```sh
root@anxin-m1:/home/anxin/iota/orchestrator# /home/anxin/distribute.sh registry.ngaiot.com/iot/iot-dac:n anxin anxin-m2

```



重启编排器（删除编排器pod）

```sh
root@anxin-m1:/home/anxin/iota/orchestrator# kubectl delete pod -niota iota-orchestrator-58f6b69fdf-56rff

```



重启DAC

```sh
kubectl delete pod -niota iota-dac-n
```

> 建议先升级个别DAC，待观察稳定，再重启升级其他dac实例