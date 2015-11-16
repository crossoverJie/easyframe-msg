package com.easyfun.easyframe.msg.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.data.Stat;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import com.easyfun.easyframe.msg.KafkaHelper;
import com.easyfun.easyframe.msg.SimpleKafkaHelper;
import com.easyfun.easyframe.msg.admin.model.BrokerModel;
import com.easyfun.easyframe.msg.admin.model.PartitionModel;
import com.easyfun.easyframe.msg.admin.model.TopicModel;

/**
 * Kafka的管理API
 * @author linzhaoming
 *
 * @Created 2014
 */
public class AdminUtil {
	
	private static ZkSerializer zkSerializer = null;
	
	/** 获取ZooKeeper的*/
	public static ZkSerializer getZkSerializer(){
		if(zkSerializer == null){
			synchronized (zkSerializer) {
				if(zkSerializer == null){
					zkSerializer = new ZkSerializer() {
						public byte[] serialize(Object data) throws ZkMarshallingError {
							return ZKStringSerializer.serialize(data);
						}
						
						public Object deserialize(byte[] bytes) throws ZkMarshallingError {
							return ZKStringSerializer.deserialize(bytes);
						}
					};
				}
			}
		}
		return zkSerializer; 
	}
	
	/** 获取ZkClient*/
	private static ZkClient getZkClient(){
		String zkConnect = KafkaHelper.getConsumerRealConfig().zkConnect();
		int connTimeout = KafkaHelper.getConsumerRealConfig().zkConnectionTimeoutMs();
		int sessTimeout = KafkaHelper.getConsumerRealConfig().zkSessionTimeoutMs();
		
		ZkClient client = new ZkClient(zkConnect, connTimeout, sessTimeout, getZkSerializer());
		return client;
	}
	
	/** 获取所有Brokers*/
	public static List<BrokerModel> getAllBrokers(){
		ZkClient client = getZkClient();
		List<Broker> asJavaList = JavaConversions.asJavaList(ZkUtils.getAllBrokersInCluster(client));
		List<BrokerModel> retList = new ArrayList<BrokerModel>();
		for (Broker broker : asJavaList) {
			BrokerModel model = new BrokerModel();
			model.setHost(broker.host());
			model.setId(broker.id());
			model.setPort(broker.port());
			retList.add(model);
		}
		return retList;
	}
	
	/**
	 * 获取Kafka的队列名字列表
	 * @return
	 */
	public static List<String> getAllTopicNames(){
		ZkClient client = getZkClient();
		Seq<String> allTopics = ZkUtils.getAllTopics(client);
		List<String> retList = JavaConversions.asJavaList(allTopics);
		return retList;
	}
	
	public static List<TopicModel> getAllTopicModels() {
		List<String> allTopicNames = getAllTopicNames();
		
		List<TopicModel> retList = new ArrayList<TopicModel>();
		
		for (String topic : allTopicNames) {
			TopicModel model = new TopicModel();
			model.setName(topic);
			
			List<PartitionModel> partitions = new ArrayList<PartitionModel>();
			List<Integer> partitionIds = getAllPartitionIds(topic);
			for (Integer partitionId : partitionIds) {
				PartitionModel partitionInfo = getPartitionInfo(topic, partitionId);
				partitions.add(partitionInfo);
			}
			
			model.setPartitions(partitions);
			
			retList.add(model);
		}
		
		return retList;
	}
	
	/**
	 * 根据指定topic获取该topic的partition列表
	 * @param topic
	 * @return
	 */
	public static List<Integer> getAllPartitionIds(String topic) {
		List list = new ArrayList();
		list.add(topic);
		Buffer buffer = JavaConversions.asScalaBuffer(list);

		Map<String, Seq<Object>> topicPartMap = JavaConversions.asJavaMap(ZkUtils.getPartitionsForTopics(getZkClient(), buffer));
		List<Object> javaList = JavaConversions.asJavaList(topicPartMap.get(topic));
		
		List<Integer> retList = new ArrayList<Integer>();
		for (Object obj : javaList) {
			retList.add((Integer)obj);
		}
		
		return retList;
	}
	
	/**
	 * 获取partition信息
	 * 
	 * @param topic
	 * @param partitionId
	 * @return
	 */
	public static PartitionModel getPartitionInfo(String topic, int partitionId) {
		PartitionModel partition = new PartitionModel();
		
		//获取Leader
		Option<Object> leaderForPartition = ZkUtils.getLeaderForPartition(getZkClient(), topic, partitionId);
		int leader = -1;
		if(leaderForPartition.isDefined()){
			leader = (Integer)leaderForPartition.get();
		}
		partition.setLeader(leader);
		
		//epoch
		int epochForPartition = ZkUtils.getEpochForPartition(getZkClient(), topic, partitionId);
		partition.setEpoch(epochForPartition);
		
		//In-Sync
		List<Object> asJavaList = JavaConversions.asJavaList(ZkUtils.getInSyncReplicasForPartition(getZkClient(), topic, partitionId));
		List<Integer> retInSyncList = new ArrayList<Integer>(asJavaList.size());
		for (Object obj : asJavaList) {
			retInSyncList.add((Integer)obj);
		}
		partition.setSyncs(retInSyncList);
		
		//replication
		List<Object> asJavaList2 = JavaConversions.asJavaList(ZkUtils.getReplicasForPartition(getZkClient(), topic, partitionId));
		List<Integer> retReplicaList = new ArrayList<Integer>(asJavaList.size());
		for (Object obj : asJavaList2) {
			retReplicaList.add((Integer)obj);
		}
		partition.setReplicass(retReplicaList);
		
		return partition;
	}
	
	/** 根据groupName获取[topic, partition]的当前offset
	 * 
	 * @param groupName
	 * @param topic
	 * @param partitionId
	 * @return
	 */
	public static String getConsumerGroupOffset(String groupName, String topic, int partitionId){
		String offsetPath = "/consumers/" + groupName +  "/offsets/" + topic + "/" + partitionId;
		Tuple2<String, Stat> readData = ZkUtils.readData(getZkClient(), offsetPath);
		String offset = readData._1;
		return offset;
	}
	
	/**
	 * 根据groupName获取[topic, partition]的当前owner
	 * @param groupName
	 * @param topic
	 * @param partitionId
	 * @return
	 */
	public static String getConsumerPartitionOwner(String groupName, String topic, int partitionId){
		String ownerPath = "/consumers/" + groupName + "/owners/" + topic + "/" + partitionId;
		Tuple2<String, Stat> readData = ZkUtils.readData(getZkClient(), ownerPath);
		String offset = readData._1;
		return offset;
	}

	/**
	 * 根据GroupName获取所有的Consumer信息
	 * @param groupName
	 * @return
	 */
	public static List<String> getAllGroupConsumers(String groupName){
		List<String> asJavaList = JavaConversions.asJavaList(ZkUtils.getConsumersInGroup(getZkClient(), groupName));
		return asJavaList;
	}
	
	/** 根据Topic列表返回TopicMetaData信息
	 * 
	 * @param topics
	 * @return */
	public static List<TopicMetadata> getTopicMetaData(List<String> topics) {
		SimpleConsumer simpleConsumer = SimpleKafkaHelper.getDefaultSimpleConsumer();
		TopicMetadataRequest metaDataRequest = new TopicMetadataRequest(topics);
		TopicMetadataResponse resp = simpleConsumer.send(metaDataRequest);
		List<TopicMetadata> metadatas = resp.topicsMetadata();

		return metadatas;
	}


	public static OffsetFetchResponse getOffsetResponse(String groupId, List<TopicAndPartition> topicAndPartitions, short versionId, int correlationId,
			String clientId) {
		SimpleConsumer simpleConsumer = SimpleKafkaHelper.getDefaultSimpleConsumer();
		OffsetFetchRequest offsetRequest = new OffsetFetchRequest(groupId, topicAndPartitions, versionId, correlationId, clientId);
		OffsetFetchResponse offsetResponse = simpleConsumer.fetchOffsets(offsetRequest);

		return offsetResponse;
	}

	public static OffsetCommitResponse commitOffsets(String groupId, Map<TopicAndPartition, OffsetMetadataAndError> requestInfo, short versionId,
			int correlationId, String clientId) {
		SimpleConsumer simpleConsumer = SimpleKafkaHelper.getDefaultSimpleConsumer();
		OffsetCommitRequest commitRequest = new OffsetCommitRequest(groupId, requestInfo, versionId, correlationId, clientId);
		OffsetCommitResponse commitResponse = simpleConsumer.commitOffsets(commitRequest);
		return commitResponse;
	}

	public static OffsetResponse getOffsetsBefore(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, short versionId, String clientId) {
		SimpleConsumer simpleConsumer = SimpleKafkaHelper.getDefaultSimpleConsumer();
		OffsetRequest offSetRequest = new OffsetRequest(requestInfo, versionId, clientId);
		OffsetResponse offsetsBefore = simpleConsumer.getOffsetsBefore(offSetRequest);
		return offsetsBefore;
	}
	
	public static void main1(String[] args) throws Exception{
		List<String> topics = new ArrayList<String>();
		topics.add("applog");
		List<TopicMetadata> topicMetaData = getTopicMetaData(topics);
		for (TopicMetadata meta : topicMetaData) {
			System.out.println(meta.topic());
			List<PartitionMetadata> list = meta.partitionsMetadata();
			System.out.println(list.size());
			for (PartitionMetadata pmeta : list) {
				System.out.println("id: " + pmeta.partitionId());
				System.out.println("isr: " + pmeta.isr());
				System.out.println("leader: " + pmeta.leader());
				System.out.println("replicas: " + pmeta.replicas());
			}
		}
	}
	
	
	public static void main(String[] args) {
		List<TopicModel> topicModels = getAllTopicModels();
		System.out.println(topicModels);
		
		
		List<BrokerModel> brokers = getAllBrokers();
		System.out.println(brokers);
	}

}
