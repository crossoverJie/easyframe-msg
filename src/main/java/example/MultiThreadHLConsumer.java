package example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.easyfun.easyframe.msg.MsgIterator;
import com.easyfun.easyframe.msg.MsgUtil;

/**
 * 测试程序: 多线程方式的Consumer
 * 
 * @author linzhaoming
 *
 * @Created 2014
 */
public class MultiThreadHLConsumer {
	private final String topic;

	public MultiThreadHLConsumer(String topic) {
		this.topic = topic;
	}

	public void testConsumer(int threadCount) throws Exception{
		List<MyThread> threadList = new ArrayList<MyThread>();
		int threadNumber = 0;
		
		List<MsgIterator> iters = MsgUtil.consume(topic, threadCount);
		for (MsgIterator iter : iters) {
			threadNumber++;
			threadList.add(new MyThread(iter, threadNumber));
		}
		
		for (int i = 0; i < threadList.size(); i++) {
			threadList.get(i).start();
		}
		for (int i = 0; i < threadList.size(); i++) {
			threadList.get(i).shutdown();
		}
		
		MsgUtil.shutdownConsummer();
	}
	
	public static class MyThread extends Thread {
		private int threadNumber;
		private CountDownLatch shutdownLatch = new CountDownLatch(1);
		MsgIterator iter;

		public MyThread(MsgIterator iter, int threadNumber) {
			this.iter = iter;
			this.threadNumber = threadNumber;
		}
		
		public void shutdown() throws Exception {
			shutdownLatch.await();
		}

		public void run() {
			Thread.currentThread().setName("EF-Msg-" + threadNumber);

			while (iter.hasNext()) {
				System.out.println("Message from thread :: " + threadNumber + " -- " + new String(iter.next()));
			}

			shutdownLatch.countDown();
		}
	}

	public static void main(String[] args) throws Exception{
		String topic = "kafkatopic";// args[0];
		int threadCount = 4;//Integer.parseInt(args[1]);
		MultiThreadHLConsumer simpleHLConsumer = new MultiThreadHLConsumer(topic);
		simpleHLConsumer.testConsumer(threadCount);
	}
}
