# EasyFrame-Msg ![Release](https://travis-ci.org/linzhaoming/easyframe-msg.svg?branch=master)
EasyFrame-Msg作为一个消息服务器的实现，包括前端调用API。目前作为Kafka客户端API的封装与代理。相对于原生的消息服务器API，使用封装的API更符合具体业务开发的特性和减低开发门槛，同时也提供高性能、稳定、可管控的功能特性  

- EasyFrame-Msg在最初版本会只支持Kafka，但封装的API与具体服务器实现无关。从而提高后续的可扩展性，如使用其它中间件，使用自身的高性能客户端实现
- EasyFrame-Msg与流处理框架EasyFrame-Stream可以做到无缝集成
- Producer与Consumer的相关参数均可通过配置调整，不需要在代码中固定死。增加系统部署调优的灵活性

----------

#EasyFrame-Msg的使用#
EasyFrame-Msg对Kafka做了封装。在使用EasyFrame-Msg进行开发的时候，主要需要调用封装的方法，不使用实际中间件的方法。避免考虑不周导致性能瓶颈并且业务代码与与实际中间件有依赖。

###使用方法###


发送消息和获取消息统一通过类com.easyfun.easyframe.msg.MsgUtil这个类进行调用，具体可参考javadoc。提供功能法如下  

- 单个发送
- 批量发送
- 默认队列发送
- 指定队列处理

### Producer例子 ###

		//1、发送指定topic名字的Topic
		String topicName = "linzm";
		for (int i = 0; i < 100; i++) {
			MsgUtil.send(topicName, "Message_" + i);
		}
		
		//2. 发送默认topic名字的Topic
		for (int i = 0; i < 100; i++) {
			MsgUtil.send("Message_" + i);
		}
		
		List list = new ArrayList();
		for(int i=0; i<10; i++){
			list.add("test_" + i);
		}
		
		//3. 批量发送Topic(指定Topic)		
		MsgUtil.send(topicName, list);
		
		//4. 批量发送Topic(默认Topic)
		MsgUtil.send(list);
		
		//关闭Producer
		MsgUtil.closeProducer();


###  Consumer例子  ###

		//消费消息 指定Topic
		String topicName = "linzm";
		MsgIterator iter = MsgUtil.consume(topicName);

		while (true) {
			if (iter.hasNext()) {
				System.out.println(iter.next());
			} else {
				continue;
			}
		}
		
		//消费消息 [默认Topic]


###Producer性能测试###
	exec ${KAFKA_HOME}/bin/kafka-run-class.sh com.easyfun.easyframe.performance.producer.PerformanceProducer $@

PerformanceProducer参数说明如下:          

    Option                                  Description                            
	------                                  -----------             
	--broker-list <hostname:port,hostname:  [REQUIRED] The *addresses* of broker
	  port>                                   lists, such as "localhost:9082".     
	--interval <Integer: size>              Interval at which to print progress     
	                                          info. (default: 10000)               
	--msg-num <Long: count>                 The number of messages to send.        
	                                          (default: 10)                        
	--msg-prefix <count>                    The *prefix* of message to send.       
	                                          (default: EasyFrameMsg)              
	--threads <Integer: number of threads>  Number of sending threads. (default: 1)
	--topics <topic1,topic2..>              [REQUIRED]: The comma separated list   
	                                          of topics to produce to. 


###Consumer性能测试###
	exec ${KAFKA_HOME}/bin/kafka-run-class.sh com.easyfun.easyframe.performance.consumer.PerformanceConsumer $@

PerformanceConsumer参数说明如下

	Option                                  Description                            
	------                                  -----------                            
	--fetch-size <Integer: size>            The amount of data to fetch in a       
	                                          single request. (default: 1048576)   
	--from-latest                           If the consumer does not already have  
	                                          an established offset to consume     
	                                          from, start with the latest message  
	                                          present in the log rather than the   
	                                          earliest message.                    
	--group <gid>                           The group id to consume on. (default:  
	                                          EasyFrame-perf-consumer-10876)       
	--interval <Integer: size>              Interval at which to print progress    
	                                          info. (default: 10000)               
	--messages <Long: count>                The number of messages to send or      
	                                          consume. (default:                   
	                                          9223372036854775807)                 
	--num-fetch-threads <Integer: count>    Number of fetcher threads. (default: 1)
	--show-detailed-stats                   If set, stats are reported for each    
	                                          reporting interval as configured by  
	                                          interval.                            
	--socket-buffer-size <Integer: size>    The size of the tcp RECV size.         
	                                          (default: 2097152)                   
	--threads <Integer: count>              Number of processing threads.          
	                                          (default: 10)                        
	--topic <topic>                         REQUIRED: The topic to consume from.   
	--zookeeper <hostname:port,hostname:    REQUIRED: The connection string for    
	  port>                                   the zookeeper connection in the form 
	                                          host:port. Multiple URLS can be      
	                                          given to allow fail-over.   



#配置文件easyframe-msg.properties例子#

easyframe-msg.properties需要放在**classpath**下，建议放在工程的config目录


	#全局配置
	global.topic.default.name=ai-topic
	global.producer.usepool=true
	
	#producer的配置
	producer.serializer.class=kafka.serializer.StringEncoder
	producer.metadata.broker.list=10.3.3.3:39092
	
	#Consumer的配置, chroot为kafka, [注意]需要与Broker的ZooKeeper配置一致
	consumer.zookeeper.connect=10.3.3.3:2801/kafka
	consumer.group.id=group1
	consumer.zookeeper.session.timeout.ms=400
	consumer.zookeeper.sync.time.ms=200
	consumer.auto.commit.interval.ms=1000


#发布文件说明#
	apidoc.rar：					EasyFrame-Msg的API
	easyframe-msg-0.0.1.jar：	EasyFrame-Msg客户端jar包
	pom.xml：					EasyFrame-Msg版本依赖情况
	example.tar：				EasyFrame-Msg使用测试代码
	easyframe-msg.properties：	EasyFrame-Msg工程配置文件，放到config目录下，需要调整对应IP地址和端口为实际部署的消息服务器对应
	aimsg.tar.gz: 				封装好的EasyFrame-Msg服务端安装tar包

#最佳实践#
	1. 	不使用Producer时，需要调用MsgUtil.closeProducer()关闭到消息服务器的连接
	2. 	不再使用Consumer时候，需要调用MsgUtil.shutdownConsumer()关闭到消息服务器的连接
	3. 	多条消息，尽量通过批量发送接口，减少网络交互与开销

#版本发布说明#
### 2014-07-08 发布第一版本###
- 提供基础功能
### 2014-07-30 发布第二版本###
- 优化性能，每次发送不关闭连接。Msgutil增加Producer和Consumer的关闭方法，客户端在不再使用的时候关闭到消息服务器的连接。
- 修复在并发并且超时情况下，MsgIterator.next()会抛出超时异常，从而导致应用异常退出。不配置超时默认使用永不超时，若需要超时处理，可以指定kafka配置，并在方法中处理Runtime异常MsgTimeoutException
- 处理多线程连接的争用问题
- 高级用法：提供指定offset消费消息队列的功能：Msgutl.fetch()方法。使用方法参考TestSimpleConsumer.java
- 高级用法：提供多线程消费消息队列的功能：使用方法参考实例程序：MultiThreadHLConsumer.java
- 提供压力测试客户端：PerformanceConsumer.java与PerformanceProducer.java，可直接运行main函数对消息服务器进行压力测试。
