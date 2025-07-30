ES索引-`WAN`



字段限制要超了？`WAN`

```json
GET  /savoir_raws/_settings?include_defaults=true

PUT /savoir_raws/_settings
{
  "index.mapping.total_fields.limit": 2000
}
```



修改局域网内指定IP机器系统

```sh
apt install nmap

nmap -O 10.8.30.109
nmap -O 10.8.30.164
nmap -O 10.8.30.143
```





安装iftop监控

```sh
sudo iftop -i eno3
```





备份ES

```sh
curl -X PUT "http://localhost:9200/_snapshot/dzy_backup" -H 'Content-Type: application/json' -d '
{
  "type": "fs",
  "settings": {
    "location": "/home/es_backup",
    "compress": true
  }
}'

```

