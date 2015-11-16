//package com.easyfun.easyframe.msg;
//
//import static org.hamcrest.CoreMatchers.equalTo;
//import static org.hamcrest.CoreMatchers.is;
//import static org.junit.Assert.assertThat;
//
//import java.util.List;
//import java.util.Properties;
//
//import kafka.javaapi.consumer.SimpleConsumer;
//import kafka.javaapi.producer.Producer;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
//
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import com.easyfun.easyframe.msg.simple.FailedFetchException;
//
//public class SimpleKafkaHelperTest {
//    private KafkaTestBroker broker;
//    private SimpleConsumer simpleConsumer;
//    
//    private static String topic = "testTopic";
//    
//    public int fetchSizeBytes = 1024 * 1024;
//    
//    @Before
//    public void setup() {
//        broker = new KafkaTestBroker();
//        String brokerStr = broker.getBrokerConnectionString();
//        System.setProperty("producer.metadata.broker.list", brokerStr);
//        simpleConsumer = SimpleKafkaHelper.getDefaultSimpleConsumer();
//    }
//    
//    @Test(expected = FailedFetchException.class)
//    public void topicDoesNotExist() throws Exception {
//    	SimpleKafkaHelper.fetch(simpleConsumer, "topic", 0, 0);
//    }
//    
////    @Test(expected = FailedFetchException.class)
////    public void brokerIsDown() throws Exception {
////        int port = broker.getPort();
////        broker.shutdown();
////        SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", port, 100, 1024, "testClient");
////        try {
////        	KafkaHelper.fetch("topic", 0, 0, 0);
////        } finally {
////            simpleConsumer.close();
////        }
////    }
//    
//	@Test
//	public void fetchMessage() throws Exception {
//		String value = "test";
//		createTopicAndSendMessage(value);
//		long offset = SimpleKafkaHelper.getLatestOffset(simpleConsumer, topic, 0) - 1;
//		List<String> fetchMsg = SimpleKafkaHelper.fetch(simpleConsumer, topic, 0, offset);
//		Assert.assertEquals(fetchMsg.size(), 1);
//		assertThat(fetchMsg.get(0), is(equalTo(value)));
//	}
//    
//    
//    @After
//    public void shutdown() throws Exception {
//    	SimpleKafkaHelper.shutdownSimpleConsummer(simpleConsumer);
//        broker.shutdown();
//    }
//    
//    private void createTopicAndSendMessage(String value) {
//        createTopicAndSendMessage(null, value);
//    }
//
//    private void createTopicAndSendMessage(String key, String value) {
//        Properties p = new Properties();
//        p.setProperty("metadata.broker.list", broker.getBrokerConnectionString());
//        p.setProperty("serializer.class", "kafka.serializer.StringEncoder");
//        ProducerConfig producerConfig = new ProducerConfig(p);
//        Producer<String, String> producer = new Producer<String, String>(producerConfig);
//        producer.send(new KeyedMessage<String, String>(topic, key, value));
//    }
//}
