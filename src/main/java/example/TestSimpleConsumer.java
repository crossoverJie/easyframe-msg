package example;

import java.util.List;
import java.util.Map;

import kafka.javaapi.consumer.SimpleConsumer;

import com.easyfun.easyframe.msg.MsgUtil;

/**
 * 测试程序：使用SimpleConsumer API
 * 
 * @author linzhaoming
 *
 * @Created 2014
 */
public class TestSimpleConsumer {
	private static String topic2 = "topic2";
	private static String topic3 = "topic3";
	
	public static class ProducerThread extends Thread {
		private final String topic;

		public ProducerThread(String topic) {
			this.topic = topic;
		}

		public void run() {
			int messageNo = 1;
			while (true) {
				MsgUtil.send(topic, "Message_" + messageNo);
				messageNo++;
			}
		}
	}

	private static void generateData() {
		ProducerThread producer2 = new ProducerThread(topic2);
		producer2.start();
		ProducerThread producer3 = new ProducerThread(topic3);
		producer3.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		generateData();
		System.out.println("Testing single fetch");
		SimpleConsumer simpleConsumer = MsgUtil.getDefaultSimpleConsumer();
		
		long offset = MsgUtil.getLatestOffset(simpleConsumer, topic2, 0);
		List<String> list1 = MsgUtil.fetch(simpleConsumer, topic2, 0, offset);
		System.out.println("Single size: " + list1.size());
		for (String str : list1) {
			System.out.println(str);
		}

		System.out.println("\n\n");
		
		System.out.println("Testing multi-fetch(Earlist)");
		
		//Earlist
		int[] partitions1 = new int[]{0, 0};
		long offset11 = MsgUtil.getLatestOffset(simpleConsumer, topic2, 0);
		long offset12 = MsgUtil.getLatestOffset(simpleConsumer, topic3, 0);
		long[] earlistOffsets1 = new long[] { offset11, offset12 };
		
		Map<String, List<String>> fetchResponse1 = MsgUtil.fetch(simpleConsumer, new String[] { topic2, topic3 }, partitions1, earlistOffsets1);
		System.out.println("Multi size(Earlist): " + fetchResponse1.size());
		for (String topic : fetchResponse1.keySet()) {
			List<String> list = fetchResponse1.get(topic);
			System.out.println("Topic: " + topic);
			for (String str : list) {
				System.out.println(str);
			}
		}
	
		System.out.println("\n\n");
		
		System.out.println("Testing multi-fetch(Latest)");
		
		//Latest
		int[] partitions2 = new int[]{0, 0};
		long offset21 = MsgUtil.getLatestOffset(simpleConsumer, topic2, 0);
		long offset22 = MsgUtil.getLatestOffset(simpleConsumer, topic3, 0);
		long[] earlistOffsets2 = new long[] { offset21, offset22 };
		
		Map<String, List<String>> fetchResponse2 = MsgUtil.fetch(simpleConsumer, new String[] { topic2, topic3 }, partitions2, earlistOffsets2);
		System.out.println("Multi size(Latest): " + fetchResponse2.size());
		for (String topic : fetchResponse2.keySet()) {
			List<String> list = fetchResponse2.get(topic);
			System.out.println("Topic: " + topic);
			for (String str : list) {
				System.out.println(str);
			}
		}
		
		MsgUtil.shutdownSimpleConsummer(simpleConsumer);
		
		System.exit(1);
	}
	
}
