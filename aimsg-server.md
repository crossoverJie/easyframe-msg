#消息服务器(aimsg-server)#
>  作为EasyFrame-Msg的一部分, aimsg-server为封装好的Kafka服务端版本  
>  aimsg-server目前支持的Kafka版本为：**Kafka 2.10 _0.8.1.1**  
>  运行消息服务器需要使用Java7以上版本, aimsg-msg自带的Java版本为: 1.7.0 _60  
>  **生产环境的ZooKeeper需要单独部署**，且使用chroot跟其它应用做隔离。Kafka自带的ZooKeeper没有高可用，不建议使用  
>  消息服务器也可以使用Kafka原生的安装介质包，原生下载介质地址：[kafka_2.10-0.8.1.1.tgz](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.8.1.1/kafka_2.10-0.8.1.1.tgz "kafka_2.10-0.8.1.1.tgz")。需要做更多的配置工作，具体安装与配置可参考[Kafka官方网站](http://kafka.apache.org/)   
>  建议使用封装好的aimsg-server服务端版本进行安装。安装介质较大，请找对应相关人员拷贝。  
>    
>  以下文档说明仅仅针对与aimsg-server的安装方法  

----------

#消息服务器(aimsg-server)的安装#
### 1. Linxu主机下新建用户aimsg 
	root#useradd -m -d /home/aimsg -s /bin/csh aimsg

### 2. 直接解压aimsg.tar.gz包    
	aimsg%tar -zxvf aimsg.tar.gz

### 3.调整配置 
配置文件：/home/aimsg/etc/server.properties    
根据实际环境需要，可调整以下配置，其中**host.name**为必需调整参数  

	port=39092  							Kafka的运行端口  
	host.name=10.3.3.3 Kafka 				Server运行的IP地址 
	log.dirs=/home/aimsg/hdata/kafka  		Kafka内部使用维护commit log的目录
	zookeeper.connect=localhost:2181 		Kafka使用的ZooKeeper地址

#启动 消息服务器(aimsg-server)#
	bin/zkStart.sh: 						启动ZooKeeper消息服务器(测试与开发)
	bin/start.sh: 							启动消息服务器

#停止 消息服务器(aimsg-server)#
	bin/stop.sh: 							停止消息服务器
	bin/zkStop.sh: 							停止ZooKeeper进程(测试与开发)

#消息服务器(aimsg-server)日常维护脚本#
	bin/tailall.sh: 						实时查看日志信息
	bin/zkShell.sh: 						运行ZooKeeper Shell
	bin/topicall.sh: 						查看Kafka的所有Topics
	bin/topicview.sh [topicName]: 			查看特定Topic的详细情况 
	bin/createtopic.sh [topicName]: 		创建Topic
	bin/deletetopic.sh [topicName]: 		删除Topic
	bin/producer.sh [topicName]: 			使用控制台发送消息到指定的Topic
	bin/consumer.sh:						使用控制台消费指定Topic的消息


#重要配置与目录#
	配置文件：/home/aimsg/etc/server.properties
	日志目录：/home/aimsg/logs/kafka
	Kafka队列log目录：/home/aimsg/hdata/kafka

#aimsg-server的目录说明
	/home/aimsg/bin							启动、停止消息服务器和日常维护脚本
	/home/aimsg/etc							消息服务器配置文件，包括kafka配置文件(server.properties)，log4j配置文件
	/home/aimsg/hdata						消息服务器内部使用保存数据的目录
	/home/aimsg/java						JDK，aimsg-server自带Java7版本
	/home/aimsg/kafka						Kafka安装包
	/home/aimsg/logs						消息服务器的运行日志目录
	
	