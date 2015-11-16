package com.easyfun.easyframe.msg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easyfun.easyframe.msg.config.EasyMsgConfig;

/** The Helper for Kafka
 * 
 * @author linzhaoming
 * 
 * @Created 2014-07-08 */
public class KafkaHelper {
	protected static Log LOG = LogFactory.getLog(KafkaHelper.class);
	
	//Config
	private static Properties globalProps = EasyMsgConfig.getProperties("global", true);
	
	private static Properties producerProps = EasyMsgConfig.getProperties("producer", true);
	private static ProducerConfig producerConfig = null;

	private static Properties consumerProps = EasyMsgConfig.getProperties("consumer", true);
	
	public static String DEFAULT_TOPIC = "ai-topic";
	
	//Producer
	
	/** Producer是否使用pool，若使用，每个线程维护一个Producer, 否则共用一个Producer
	 * <li>默认为不使用，即所有线程共用一个Producer
	 * */
	private static Boolean IS_PRODUCER_USE_POOL = false;
	
	/** [使用pool] 的ThreadLocal的Producer信息*/
	private static final ThreadLocal<Producer<Object, String>> PRODUCER_LOCAL = new ThreadLocal<Producer<Object,String>>();
	/** [不使用pool] 的全局Producer信息*/
	private static Producer<Object, String> globalProducer = null;
	private static Lock producerLock = new ReentrantLock();
	
	
	//Consumer
	private static final String DEFAULT_GROUP_ID;
	
	/** Key为GroupId，Value为对应的ConsumerConfig*/
	private static Map<String, ConsumerConfig> groupConsumerConfigs = new HashMap<String, ConsumerConfig>();
	/** Key为GroupId，Value为对应的ConsumerConnector*/
	private static Map<String, ConsumerConnector> groupConsumers = new HashMap<String, ConsumerConnector>();
	private static Lock consumerLock = new ReentrantLock();
	
//	//SimpleConsumer
//	private static SimpleConsumer globalSimpleConsumer = null;
//	private static Lock simpleConsumerLock = new ReentrantLock();
//	private static final String SIMPLECONSUMER_CLIENT_NAME = "EasyFrameSimpleClient";
//	
//	private static final int NO_OFFSET = -5;

	static {
		String strDefaultTopic = globalProps.getProperty("topic.default.name");
		if (!StringUtils.isBlank(strDefaultTopic)) {
			DEFAULT_TOPIC = strDefaultTopic;
		}
		//JVM属性优先，覆盖配置文件的配置
		String strProperty = System.getProperty("global.topic.default.name");
		if(StringUtils.isNotBlank(strProperty)){
			DEFAULT_TOPIC = strProperty;
		}
		if(LOG.isInfoEnabled()){
			LOG.info("[EF-Msg] " + "global: default topic is: " + DEFAULT_TOPIC);
		}
		
		String strproducerUsePool = globalProps.getProperty("producer.usepool");
		if (StringUtils.equalsIgnoreCase("true", strproducerUsePool)) {
			IS_PRODUCER_USE_POOL = true;
		}
		if(LOG.isInfoEnabled()){
			LOG.info("[EF-Msg] " + "global: producerUsePool: " + IS_PRODUCER_USE_POOL);
		}

		//Producer
		handleJVMProperties("producer", "metadata.broker.list", producerProps);
		
		//producer.serializer.class
		handleJVMProperties("producer", "serializer.class", producerProps);
		
		//Consumer
		//consumer.zookeeper.connect
		handleJVMProperties("consumer", "zookeeper.connect", consumerProps);
		
		//consumer.group.id
		handleJVMProperties("consumer", "group.id", consumerProps);
		
		//consumer.socket.receive.buffer.bytes
		handleJVMProperties("consumer", "socket.receive.buffer.bytes", consumerProps);
		
		//consumer.fetch.message.max.bytes
		handleJVMProperties("consumer", "fetch.message.max.bytes", consumerProps);
		
		//consumer.auto.offset.reset
		handleJVMProperties("consumer", "auto.offset.reset", consumerProps);
		
		//consumer.consumer.timeout.ms
		handleJVMProperties("consumer", "consumer.timeout.ms", consumerProps);
		//默认设置永不超时
		if(StringUtils.isBlank(consumerProps.getProperty("consumer.timeout.ms"))){
			consumerProps.setProperty("consumer.timeout.ms", "-1");
		}
		
		//consumer.num.consumer.fetchers
		handleJVMProperties("consumer", "num.consumer.fetchers", consumerProps);
		
		
		//特殊处理"group.id"
		if(StringUtils.isBlank(consumerProps.getProperty("group.id"))){
			LOG.info("[EF-Msg] " + "global: groupId is null, set the default= (" + "EasyFrameGroup" + ")");
			consumerProps.setProperty("group.id", "EasyFrameGroup");
		}
		
		DEFAULT_GROUP_ID = consumerProps.getProperty("group.id");
		
		if(LOG.isDebugEnabled()){
			LOG.debug("[EF-Msg] Consumer Config(global) " + consumerProps);
			LOG.debug("[EF-Msg] Producer Config(global): " + producerProps);
		}

	}
	
	/**
	 * 读取JVM的属性，设置到Properties中
	 * @param jvmKey JVM的属性
	 * @param propKey Properties的属性
	 */
	private static void handleJVMProperties(String jvmPrefix, String propKey, Properties prop){
		String jvmKey = jvmPrefix + "." + propKey;
		String strProperty = System.getProperty(jvmKey);
		if(StringUtils.isNotBlank(strProperty)){
			prop.setProperty(propKey, strProperty);
		}
	}
	
	//############### Producer逻辑 #########################
	
	private static synchronized void initProducerConfig() {
		if (producerConfig == null) {
			try {
				producerConfig = new ProducerConfig(producerProps);
			} catch (Exception e) {
				LOG.error("初始化Producer出错: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/** 获取 Producer 
	 * <li>K: Typed of the optional key associated with the message from message partition
	 * <li>V: Typed of the message
	 * */
	static Producer<Object, String> getProducer() {
		if(IS_PRODUCER_USE_POOL) {
			Producer<Object, String> producer = PRODUCER_LOCAL.get();
			//使用ThreadLocal
			if (producer == null) {
				if (producerConfig == null) {
					initProducerConfig();
				}
				producer = new Producer<Object, String>(producerConfig);
				PRODUCER_LOCAL.set(producer);
			}
			
			return producer;
		}else{
			if (globalProducer == null) {
				try{
					producerLock.lock();
					if (globalProducer == null){
						if (producerConfig == null) {
							initProducerConfig();
						}
						globalProducer = new Producer<Object, String>(producerConfig);
					}
				}finally{
					producerLock.unlock();
				}
			}
			
			return globalProducer;
		}
	}
	
	/** 发送单条消息 [指定Topic]
	 * 
	 * @param topicName 队列名称
	 * @param msg 消息内容 
	 * @param partKey 分区对象 为空，表示使用消息内容作为Key分区
	 * */
	static void send(String topicName, String msg, Object partKey) {
		Producer<Object, String> producer = KafkaHelper.getProducer();
		
		KeyedMessage<Object, String> message = null;
		if (partKey == null) {
			// 将消息内容作为分区Key
			message = new KeyedMessage<Object, String>(topicName, null, msg, msg);
		} else {
			message = new KeyedMessage<Object, String>(topicName, null, partKey, msg);
		}
		
		//发送数据到单个topic, 使用同步或异步的方式, 可以由Key分区
		long start = System.currentTimeMillis();
		producer.send(message);
		if(LOG.isDebugEnabled()){
			long end = System.currentTimeMillis();
			LOG.debug("Sent [" + message + "]" + ", cost = [" + (end-start) + "]");
		}
	}
	
	/** 同时发送多条消息  [指定Topic]
	 * 
	 * @param topicName 队列名称
	 * @param msgs 消息内容列表
	 * @param partKeys 分区键对象列表
	 *  */
	static void send(String topicName, List<String> msgs, List<Object> partKeys) {
		boolean withPartKeys = false;
		if (partKeys == null || partKeys.size() == 0) {
			withPartKeys = false;
		} else {
			withPartKeys = true;
			// Check
			if (msgs.size() != partKeys.size()) {
				throw new IllegalArgumentException("The msgs list and partKeys list is not the same size");
			}
		}
		
		Producer<Object, String> producer = KafkaHelper.getProducer();

		List<KeyedMessage<Object, String>> messages = new ArrayList<KeyedMessage<Object, String>>();
		for(int i=0; i<msgs.size(); i++){
			String msg = msgs.get(i);
			if (withPartKeys) {
				Object partkey = partKeys.get(i);
				messages.add(new KeyedMessage<Object, String>(topicName, null, partkey, msg));
			} else {
				// 将消息内容作为分区Key
				messages.add(new KeyedMessage<Object, String>(topicName, null, msg, msg));
			}
		}

		//发送数据列表到多条Topics, 使用同步或异步的方式, 可以由Key分区
		producer.send(messages);
	}
	
	static void closeProducer() {
		//关闭到所有Kafka Brokers的连接池，如果有到ZooKeeper client的连接，也会关闭
		if(IS_PRODUCER_USE_POOL) {
			Producer<Object, String> producer = PRODUCER_LOCAL.get();
			if (producer != null) {
				if(LOG.isDebugEnabled()){
					LOG.debug("Closing Producer" + producer);
				}
				producer.close();
				producer = null;
				PRODUCER_LOCAL.set(null);
			}
		}else{
			if (globalProducer != null) {
				try {
					producerLock.lock();
					if (globalProducer != null){
						if(LOG.isDebugEnabled()){
							LOG.debug("Closing Producer" + globalProducer);
						}
						globalProducer.close();
						globalProducer = null;
					}
				} finally {
					producerLock.unlock();
				}
			}
		}
	}
	
	//################## Consumer逻辑 ########################
	/**消费消息  [指定Topic]
	 * 
	 * @param topicName 队列名称
	 * @param groupId Group Name
	 * @return
	 */
	static MsgIterator consume(String topicName, String groupId) {
		ConsumerConnector consumerConnector = KafkaHelper.getConsumer(groupId);
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();	//(topic, #stream) pair
		topicCountMap.put(topicName, new Integer(1));

		//TODO: 可消费多个topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);	//Using default decoder
		List<KafkaStream<byte[], byte[]>> streamList = consumerMap.get(topicName);	//The number of items in the list is #streams, Each Stream supoorts an iterator over message/metadata pair
		KafkaStream<byte[], byte[]> stream = streamList.get(0);
		
		//KafkaStream[K,V] K代表partitio Key的类型，V代表Message Value的类型
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		MsgIterator iter = new MsgIterator(it);
		return iter;
	}
	
	
	/**消费消息  [指定Topic] 指定线程
	 * 
	 * @param topicName 队列名称
	 * @param numStreams Number of streams to return
	 * @return A list of MsgIterator each of which provides an iterator over message over allowed topics
	 */
	static List<MsgIterator> consume(String topicName, int numStreams) {
		return consume(topicName, numStreams, DEFAULT_GROUP_ID);
	}
	
	/**消费消息  [指定Topic] 指定线程
	 * 
	 * @param topicName 队列名称
	 * @param numStreams Number of streams to return
	 * @return A list of MsgIterator each of which provides an iterator over message over allowed topics
	 */
	static List<MsgIterator> consume(String topicName, int numStreams, String groupId) {
		ConsumerConnector consumerConnector = KafkaHelper.getConsumer(groupId);
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();	//(topic, #stream) pair
		topicCountMap.put(topicName, numStreams);

		//TODO: 可消费多个topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);	//Using default decoder
		List<KafkaStream<byte[], byte[]>> streamList = consumerMap.get(topicName);	//The number of items in the list is #streams, Each Stream supoorts an iterator over message/metadata pair
		
		List<MsgIterator> iterList = new ArrayList<MsgIterator>();
		for (KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			MsgIterator iter = new MsgIterator(it);
			iterList.add(iter);
		}
		
		//KafkaStream[K,V] K代表partitio Key的类型，V代表Message Value的类型
		return iterList;
	}
	
	static void shutdownConsummer(String groupId){
		String consumerKey = groupId + "|" + Thread.currentThread().getName();
		ConsumerConnector consumerConnector = groupConsumers.get(consumerKey);
		try{
			consumerLock.lock();
			consumerConnector = groupConsumers.get(consumerKey);
			if (consumerConnector != null ) {
				consumerConnector.shutdown();
				groupConsumers.remove(consumerKey);
				consumerConnector = null;
			}
		}finally{
			consumerLock.unlock();
		}
	}
	
	static void commitConsumerOffsets(String groupId) {
		String consumerKey = groupId + "|" + Thread.currentThread().getName();
		ConsumerConnector consumerConnector = groupConsumers.get(consumerKey);
		if (consumerConnector != null) {
			consumerConnector.commitOffsets();
			LOG.debug("[EF-Msg] Commit: " + groupId);
		}
	}
	
	public static ConsumerConnector getConsumer(String groupId) {
		//加上线程名字的考虑是：保证每个线程只有一个Consumer，但是每个线程又可以有一个独立的Consumer，从而消费不同的partition
		String consumerKey = groupId + "|" + Thread.currentThread().getName();
		ConsumerConnector msgConnector = groupConsumers.get(consumerKey);
		if (msgConnector == null) {
			try {
				consumerLock.lock();
				msgConnector = groupConsumers.get(consumerKey);
				if (msgConnector == null) {
					msgConnector = Consumer.createJavaConsumerConnector(getConsumerRealConfig(groupId));
					groupConsumers.put(consumerKey, msgConnector);
				}
			} finally {
				consumerLock.unlock();
			}
		}

		return msgConnector;
	}

	/** 获取Producer特定配置*/
	static String getProducerConfig(String key){
		return producerProps.getProperty(key);
	}
	
	/** 获取配置的Group Id*/
	public static String getDefaultGroupId(){
		return DEFAULT_GROUP_ID;
	}
	
	/** 获取配置文件指定的Consumer配置*/
	static String getConsumerConfiguration(String key){
		return consumerProps.getProperty(key);
	}
	
	public static ConsumerConfig getConsumerRealConfig(String groupId) {
		ConsumerConfig consumerConfig = groupConsumerConfigs.get(groupId);
		if (consumerConfig == null) {
			Properties groupProp = new Properties(consumerProps);
			groupProp.putAll(consumerProps);
			groupProp.put("group.id", groupId);
			consumerConfig = new ConsumerConfig(groupProp);
			groupConsumerConfigs.put(groupId, consumerConfig);
		}

		return consumerConfig;
	}
	
	public static ProducerConfig getProducerRealConfig(){
		if (producerConfig == null) {
			initProducerConfig();
		}
		return producerConfig;
	}
	
	public static ConsumerConfig getConsumerRealConfig(){
		return getConsumerRealConfig(DEFAULT_GROUP_ID);
	}
	
	public static void main(String[] args) {
	}
}
