package com.easyfun.easyframe.msg;

import java.util.List;
import java.util.Map;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** The MsgUtil will handle the sending and consuming of the messages in the queue.
 * 
 * @author linzhaoming
 * 
 * 
 * @Created 2014-07-08 */
public class MsgUtil {
	protected static Log LOG = LogFactory.getLog(MsgUtil.class);
	
	/**
	 * 获取默认发送Topic名字
	 * @return
	 */
	public static String getDefaultTopic() {
		return KafkaHelper.DEFAULT_TOPIC;
	}
	
	/**
	 * 获取配置的默认groupId
	 * @return
	 */
	public static String getDefaultGroupId(){
		return KafkaHelper.getDefaultGroupId();
	}
	
	
	//########################## Producer  ##########################
	
	/** 发送单条消息 [使用配置的默认Topic]
	 * 
	 * @param msg 消息内容 */
	public static void send(String msg) {
		send(getDefaultTopic(), msg);
	}

	/** 发送单条消息, [使用配置的默认Topic]
	 * 
	 * @param msg 消息内容 */
	public static void send(List<String> msgs) {
		send(getDefaultTopic(), msgs);
	}

	/** 发送单条消息 [指定Topic]
	 * 
	 * @param topicName 队列名称
	 * @param msg 消息内容 */
	public static void send(String topicName, String msg) {
		KafkaHelper.send(topicName, msg, null);
	}
	
	/** 发送单条消息 [指定Topic]
	 * 
	 * @param topicName 队列名称
	 * @param msg 消息内容
	 * @param partKey 分区字段
	 *  */
	public static void send(String topicName, String msg, Object partKey) {
		KafkaHelper.send(topicName, msg, partKey);
	}

	/** 同时发送多条消息  [指定Topic]
	 * 
	 * @param topicName 队列名称
	 * @param msgs 消息内容列表 */
	public static void send(String topicName, List<String> msgs) {
		KafkaHelper.send(topicName, msgs, null);
	}
	
	/** 同时发送多条消息  [指定Topic]
	 * 
	 * @param topicName 队列名称
	 * @param msgs 消息内容列表 
	 * @param partKeys 分区对象列表值 List为空或null表示不使用自定义分区对象
	 * */
	public static void send(String topicName, List<String> msgs, List<Object> partKeys) {
		KafkaHelper.send(topicName, msgs, partKeys);
	}
	
	/** 在程序退出之前或不再使用Kafka发送消息之后，需要调用这个方法关闭到Kafka的连接*/
	public static void closeProducer(){
		KafkaHelper.closeProducer();
	}
	
	/** 获取Producer的描述信息*/
	public static String getProducerDesc() {
		ProducerConfig config = KafkaHelper.getProducerRealConfig();
		StringBuffer sb = new StringBuffer();
		sb.append("brokers=(").append(config.brokerList()).append(")");
		sb.append(", clientId=(").append(config.clientId()).append(")");
		sb.append(", producerType=(").append(config.producerType()).append(")");
		sb.append(", partitionerClass=").append(config.partitionerClass()).append(")");
		sb.append(", serializerClass=(").append(config.serializerClass()).append(")");
		sb.append(", keySerializerClass=(").append(config.keySerializerClass()).append(")");
		return sb.toString();
	}
	
	//########################## Consumer  ##########################
	/**消费消息  [指定Topic]
	 * 
	 * @param topicName 队列名称
	 * @return A MsgIterator object which provides an iterator over message over allowed topics
	 */
	public static MsgIterator consume(String topicName) {
		return KafkaHelper.consume(topicName, KafkaHelper.getDefaultGroupId());
	}
	
	/**消费消息  [指定Topic和Group]
	 * 
	 * @param topicName 队列名称
	 * @param groupId Group Name
	 * @return A MsgIterator object which provides an iterator over message over allowed topics
	 */
	public static MsgIterator consume(String topicName, String groupId) {
		return KafkaHelper.consume(topicName, groupId);
	}
	
	/**消费消息  [指定Topic] 分为多个Stream的形式
	 * 
	 * @param topicName 队列名称
	 * @param numStreams Number of streams to return
	 * 
	 * @return A list of MsgIterator each of which provides an iterator over message over allowed topics
	 */
	public static List<MsgIterator> consume(String topicName, int numStreams) {
		return KafkaHelper.consume(topicName, numStreams);
	}
	
	/**
	 * 消费消息，[使用配置的默认Topic]
	 * @return A MsgIterator object which provides an iterator over message over allowed topics
	 */
	public static MsgIterator consume() {
		return KafkaHelper.consume(getDefaultTopic(), KafkaHelper.getDefaultGroupId());
	}
	
	/** Commit the offsets of all topic/partitions connected by this connector */
	public static void commitConsumerOffsets(){
		KafkaHelper.commitConsumerOffsets(KafkaHelper.getDefaultGroupId());
	}
	
	/** Commit the offsets of all topic/partitions connected by this connector 
	 *  @param groupId Group Name
	 * */
	public static void commitConsumerOffsets(String groupId){
		KafkaHelper.commitConsumerOffsets(groupId);
	}
	
	/** 在程序退出之前或不再使用Kafka消费消息之后，需要调用这个方法关闭到Kafka的Consumer连接*/
	public static void shutdownConsummer(){
		KafkaHelper.shutdownConsummer(KafkaHelper.getDefaultGroupId());
	}
	
	/** 在程序退出之前或不再使用Kafka消费消息之后，需要调用这个方法关闭到Kafka的Consumer连接
	 *  @param groupId Group Name
	 * */
	public static void shutdownConsummer(String groupId){
		KafkaHelper.shutdownConsummer(groupId);
	}
	
	/** 获取配置文件指定的Consumer配置*/
	public static String getConsumerConfig(String key){
		return KafkaHelper.getConsumerConfiguration(key);
	}
	
	/** 获取Consumer的描述信息*/
	public static String getConsumeDesc(){
		String groupId = getDefaultGroupId();
		ConsumerConfig consumerRealConfig = KafkaHelper.getConsumerRealConfig(groupId);
		return "ZKConnect=(" + consumerRealConfig.zkConnect() + "), groupId=(" + groupId + "), clientId=(" 
			+ consumerRealConfig.clientId() + "), autoOffsetReset=(" + consumerRealConfig.autoOffsetReset() + ")";
	}
	
	/** 获取Consumer的描述信息
	 *  @param groupId Group Name
	 * */
	public static String getConsumeDesc(String groupId){
		ConsumerConfig consumerRealConfig = KafkaHelper.getConsumerRealConfig(groupId);
		return "ZKConnect=(" + consumerRealConfig.zkConnect() + "), groupId=(" + groupId + "), clientId=(" 
			+ consumerRealConfig.clientId() + "), autoOffsetReset=(" + consumerRealConfig.autoOffsetReset() + ")";
	}

	//########################## SimpleConsumer  ##########################
	
	public static SimpleConsumer getDefaultSimpleConsumer() {
		return SimpleKafkaHelper.getDefaultSimpleConsumer();
	}
	
	public static SimpleConsumer getSimpleConsumer(String host, int port) {
		return SimpleKafkaHelper.getSimpleConsumer(host, port);
	}
	
	public static long getEarlierOffset(SimpleConsumer simpleConsumer, String topic, int partition) {
		return SimpleKafkaHelper.getEarlierOffset(simpleConsumer, topic, partition);
	}
	
	public static long getLatestOffset(SimpleConsumer simpleConsumer, String topic, int partition) {
		return SimpleKafkaHelper.getLatestOffset(simpleConsumer, topic, partition);
	}
	
	/** 在程序退出之前或不再使用Kafka消费消息之后，需要调用这个方法关闭到Kafka的SimpleConsumer连接*/
	public static void shutdownSimpleConsummer(SimpleConsumer simpleConsumer){
		SimpleKafkaHelper.shutdownSimpleConsummer(simpleConsumer);
	}
	
	/** 返回消费的消息List, 指定offset
	 * 
	 * @param topic The topic name
	 * @param partition Topic position
	 * @param offset	Starting byte offset
	 * @return
	 * @throws Exception
	 */
	public static List<String> fetch(SimpleConsumer simpleConsumer, String topic, int partition, long offset) throws Exception{
		return SimpleKafkaHelper.fetch(simpleConsumer, topic, partition, offset);
	}
	
	/** 返回消费的消息Map, 指定offset
	 * <li>(Key为Topic name, Value为返回消息的消息List
	 * 
	 * @param topics The topic names
	 * @param partitions Topic position
	 * @param offsets	Starting byte offset
	 * @return
	 * @throws Exception
	 */
	public static Map<String, List<String>> fetch(SimpleConsumer simpleConsumer, String[] topics, int[] partitions, long[] offsets) throws Exception{
		return SimpleKafkaHelper.fetch(simpleConsumer, topics, partitions, offsets);
	}
}
