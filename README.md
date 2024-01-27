# dremio-highgo-plugin
dremio瀚高连接插件
1. pom文件中指定dremio版本
2. mvn打包后 将该包复制到dremio容器中 路径为 /opt/dremio/jars/ ；
3. 将瀚高驱动复制到 /opt/dremio/jars/3rdparty
4. 重启dremio

连接数据源时要填写的内容与PostgreSQL完全相同
![img.png](doc/img.png)