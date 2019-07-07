# mysqlkafka
通过mysql的binlog,将数据实时导入kafka

1.mysql开启binlog  
登录MySQL后，输入show variables like '%log_bin%' 查看binlog日志是否为OFF关闭状态;  
如果是 退出MySQL，使用vi编辑器修改MySQL的my.cnf配置文件(Linux下)  
加入  
[mysqld]  
log-bin = /usr/local/var/mysql/logs/mysql-bin.log #设置日志路径，注意路经需要mysql用户有权限写  
expire-logs-days = 14 #设置binlog清理时间  
max-binlog-size = 500M #binlog每个日志文件大小  
server-id = 1  
保存重启mysql服务,再次登录MySQL，输入show variables like '%log_bin%' 查看是都启用  
  
2.安装kafka  
通过官网下载,安装到服务器上(Linux)  
配置server.properties 下面两句话默认注释  
listeners=PLAINTEXT://当前主机的hostname:9092  
advertised.listeners=PLAINTEXT://当前主机的hostname:9092  
启动zookeeper  
bin/zookeeper-server-start.sh config/zookeeper.properties  
启动kafka  
bin/kafka-server-start.sh config/server.properties &  
kafka新建主题  
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mykafka 
Created topic mykafka.  
kafka生产者测试  
./kafka-console-producer.sh --broker-list localhost:9092 --topic mykafka  
kafka消费者测试  
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mykafka  

3.编写代码  
IDEA clone 本页代码 https://github.com/dzjfromChina/mysqlkafka.git  
配置你的kafka ip topic等信息  
配置你的源头数据库信息  
启动MysqlkafkaApplication.java这个类  
启动KafkaConsumerDemo.java这个 用于消费kafka信息  
数据库进行增删改   
 