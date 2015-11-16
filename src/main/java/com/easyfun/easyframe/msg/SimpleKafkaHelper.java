package com.easyfun.easyframe.msg;

import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import scala.collection.JavaConversions;

import com.easyfun.easyframe.msg.simple.BrokerInfo;
import com.easyfun.easyframe.msg.simple.FailedFetchException;
import com.easyfun.easyframe.msg.simple.KafkaError;

public class SimpleKafkaHelper {
	private static Log LOG = LogFactory.getLog(SimpleKafkaHelper.class);
	
	private static final String SIMPLECONSUMER_CLIENT_NAME = "EasyFrameSimpleClient";
	
	private static int FETCH_SIZE_BYTES = 1024 * 1024;
	
	private static final int NO_OFFSET = -5;
	
	
	//################## SimpleConsumer逻辑 ########################
	
	private static BrokerInfo getDefaultBroker(){
		//1. 先查找producer
		String host = null;
		int port = 9082;
		boolean isFound = false;
		String brokerList = KafkaHelper.getProducerConfig("metadata.broker.list"); //KafkaHelper.getProducerRealConfig().brokerList();
		if(StringUtils.isNotBlank(brokerList)){
			String[] split = brokerList.split(",");
			if(split.length >0){
				String[] tmp = split[0].split(":");
				host = tmp[0];
				port = Integer.parseInt(tmp[1]);
				isFound = true;
			}
		}
		
		//2. 在查找consumer
		if(isFound == false){
			//从ZK获取Client
			String zkConnect = KafkaHelper.getConsumerRealConfig().zkConnect();
			int connTimeout = KafkaHelper.getConsumerRealConfig().zkConnectionTimeoutMs();
			int sessTimeout = KafkaHelper.getConsumerRealConfig().zkSessionTimeoutMs();
			
			ZkSerializer zkSerializer = new ZkSerializer() {
				public byte[] serialize(Object data) throws ZkMarshallingError {
					return ZKStringSerializer.serialize(data);
				}
				
				public Object deserialize(byte[] bytes) throws ZkMarshallingError {
					return ZKStringSerializer.deserialize(bytes);
				}
			};
			
			ZkClient client = new ZkClient(zkConnect, connTimeout, sessTimeout, zkSerializer);
			
			List<Broker> asJavaList = JavaConversions.asJavaList(ZkUtils.getAllBrokersInCluster(client));
			if (asJavaList.size() > 0) {
				Broker broker = asJavaList.get(0);
				host = broker.host();
				port = broker.port();
				isFound = true;
			}
		}
		
		if(isFound){
			BrokerInfo broker = new BrokerInfo(host, port);
			return broker;
		}else{
			return null;
		}
	}
	
	/** 获取SimpleConsumer
	 *  <li>相对于ConsumerConnector, SimpleConsumer的使用层次更低级别
	 *  <li>主要用于实现High-level API和由某些离线应用直接调用(如Hadoop Consumer), 因为这些应用有维护状态的特定需求
	 * */
	public static SimpleConsumer getDefaultSimpleConsumer() {
		BrokerInfo brokerInfo = getDefaultBroker();
		if (brokerInfo != null) {
			return getSimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort());
		} else {
			return null;
		}
	}
	
	public static SimpleConsumer getSimpleConsumer(String host, int port) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("[EF-Msg] 初始化SimpleConsumer: host=(" + host + "), port=(" + port + ")");
		}

		int soTimeout = 30 * 1000; // "consumer.socket.timeout.ms"
		int bufferSize = 64 * 1024; // "consumer.socket.receive.buffer.bytes"
		SimpleConsumer globalSimpleConsumer = new SimpleConsumer(host, port, soTimeout, bufferSize, SIMPLECONSUMER_CLIENT_NAME);

		return globalSimpleConsumer;
	}
	
	static void shutdownSimpleConsummer(SimpleConsumer simpleConsumer) {
		if (simpleConsumer != null) {
			simpleConsumer.close();
		}
	}
	
	/**
	 * 根据topic, partition, offset, fetchSize构建FetchRequest
	 * 
	 * @param clientId
	 * @param topic		The topic name
	 * @param partition	Topic partition
	 * @param offset	Starting byte offset
	 * @return
	 */
	private static FetchRequest getFetchRequest(SimpleConsumer simpleConsumer, String topic, int partition, long offset){
		FetchRequestBuilder builder = new FetchRequestBuilder().clientId(SIMPLECONSUMER_CLIENT_NAME);
		
		builder.addFetch(topic, partition, offset, FETCH_SIZE_BYTES);
		
		return builder.build(); //partition=0, offset=0, fetchsize=100
	}
	
	/**根据topic列表, partition, offset, fetchSize构建FetchRequest
	 * 
	 * @param clientId
	 * @param topic		The topic name
	 * @param partition	Topic partition
	 * @param offset	Starting byte offset
	 * @return
	 */
	private static FetchRequest getFetchRequest(SimpleConsumer simpleConsumer, String[] topics, int[] partitions, long[] offset){
		if (topics.length != offset.length || topics.length != partitions.length) {
			throw new RuntimeException("The length of (topics/offsets/partitions) must be equal.");
		}
		FetchRequestBuilder builder = new FetchRequestBuilder().clientId(SIMPLECONSUMER_CLIENT_NAME);
		
		for (int i = 0; i < topics.length; i++) {
			builder.addFetch(topics[i], partitions[i], offset[i], FETCH_SIZE_BYTES); // partition=0, offset=0, fetchsize=100
		}
		
		return builder.build();
	}
	
	/** 返回消费的消息List, 指定offset
	 * 
	 * @param topic The topic name
	 * @param partition Topic position
	 * @param offset	Starting byte offset
	 * @return
	 * @throws Exception
	 */
	static List<String> fetch(SimpleConsumer simpleConsumer, String topic, int partition, long offset) throws Exception{
		List<String> retList = new ArrayList<String>();
		FetchRequest fetchRequest = getFetchRequest(simpleConsumer,topic, partition, offset);
		
		FetchResponse fetchResponse = null;
		try {
			fetchResponse = simpleConsumer.fetch(fetchRequest);
		} catch (Exception e) {
			 if (e instanceof ConnectException) {
                 throw new FailedFetchException(e);
             } else {
                 throw new RuntimeException(e);
             }
		}
		
		if (fetchResponse.hasError()) {
			KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partition));
			String message = "Error fetching data from [" + partition + "] for topic [" + topic + "]: [" + error + "]";
			LOG.error(message);
			throw new FailedFetchException(message);
		}
		
		ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partition);
		
		for (MessageAndOffset messageAndOffset : messageSet) {
			ByteBuffer payload = messageAndOffset.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			String msg = new String(bytes, "UTF-8");
			retList.add(msg);
		}
		
		return retList;
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
	static Map<String, List<String>> fetch(SimpleConsumer simpleConsumer, String[] topics, int[] partitions, long[] offsets) throws Exception{
		FetchRequest fetchRequest = getFetchRequest(simpleConsumer,topics, partitions, offsets);
		FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
		
		Map<String, List<String>> retMap = new HashMap<String, List<String>>();
		for (int i = 0; i < topics.length; i++) {
			String topic = topics[i];
			List list = new ArrayList<String>();
			retMap.put(topic, list);
			
			ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partitions[i]);
			
			for (MessageAndOffset messageAndOffset : messageSet) {
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				String msg = new String(bytes, "UTF-8");
				list.add(msg);
			}
		}
		
		return retMap;
	}
	
    private static long getOffset(SimpleConsumer simpleConsumer, String topic, int partition, long startOffsetTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());

        long[] offsets = simpleConsumer.getOffsetsBefore(request).offsets(topic, partition);
        if (offsets.length > 0) {
            return offsets[0];
        } else {
            return NO_OFFSET;
        }
    }
    
    /** 获取指定Topic的最早Offset*/
    public static long getEarlierOffset(SimpleConsumer simpleConsumer, String topic, int partition) {
    	return getOffset(simpleConsumer, topic, partition, kafka.api.OffsetRequest.EarliestTime());
    }
    
    /** 获取指定Topic的最近Offset*/
    public static long getLatestOffset(SimpleConsumer simpleConsumer, String topic, int partition) {
    	return getOffset(simpleConsumer, topic, partition, kafka.api.OffsetRequest.LatestTime());
    }
}
