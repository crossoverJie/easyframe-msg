package com.easyfun.easyframe.performance.consumer;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import kafka.utils.ZkUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easyfun.easyframe.msg.MsgIterator;
import com.easyfun.easyframe.msg.MsgUtil;

/** The main program of Consumer Perf.
 * 
 * 
 * @author linzhaoming
 * 
 * @Created 2014-07-26 */
public class PerformanceConsumer {

	private static Log LOG = LogFactory.getLog(PerformanceConsumer.class);

	private static DecimalFormat FORMATTER = new DecimalFormat("#,##0.00");

	public static void main(String[] args) throws Exception {
		PerfConsumerConfig config = new PerfConsumerConfig(args);
		LOG.info("Starting consumer ...");
		
		AtomicLong totalMessagesRead = new AtomicLong(0);	//读取的所有消息数量
		AtomicLong totalBytesRead = new AtomicLong(0);		//读取的所有字节数

		// 清除Perf Run程序中的ZooKeeper状态
		ZkUtils.maybeDeletePath(MsgUtil.getConsumerConfig("zookeeper.connect"), "/consumers/" + MsgUtil.getConsumerConfig("group.id"));
		
		List<PerfConsumerThread> threadList = new ArrayList<PerfConsumerThread>();
		
		long startMs = System.currentTimeMillis();
		Runtime.getRuntime().addShutdownHook(new ShutdownThread(startMs, totalBytesRead, totalMessagesRead, config));
		
		List<MsgIterator> iterList = MsgUtil.consume(config.topic, config.numThreads);

		for (int i = 0; i < iterList.size(); i++) {
			MsgIterator iter = iterList.get(i);
			threadList.add(new PerfConsumerThread(i, "EF-Msg-Consumer-" + i, iter, config, totalMessagesRead, totalBytesRead));
		}

		LOG.info("Sleeping for 1 second.");
		Thread.sleep(1000);
		LOG.info("starting threads");

		
		for (int i = 0; i < threadList.size(); i++) {
			threadList.get(i).start();
		}
		for (int i = 0; i < threadList.size(); i++) {
			threadList.get(i).shutdown();
		}

		System.exit(0);
	}
	
	private static class ShutdownThread extends Thread {
		private long startMs;
		private AtomicLong totalBytesRead;
		private AtomicLong totalMessagesRead;
		private PerfConsumerConfig config;

		public ShutdownThread(long startMs, AtomicLong totalBytesRead, AtomicLong totalMessagesRead, PerfConsumerConfig config) {
			this.startMs = startMs;
			this.totalBytesRead = totalBytesRead;
			this.totalMessagesRead = totalMessagesRead;
			this.config = config;
		}
		
		public void run() {
			long elapsed = System.currentTimeMillis() - startMs;
			printMessage("[EF-Msg Consumer]: ", totalBytesRead.get(), totalMessagesRead.get(), elapsed, true);
			
			config.clearConsumer();
		}
	}
	
	public static void printMessage(String name, long bytesRead, long messagesRead, long elapsed, boolean isEnd) {
		String totalMBRead = FORMATTER.format((bytesRead * 1.0) / (1024 * 1024));
		String avgMBRead = FORMATTER.format(1000.0 * (bytesRead / elapsed)/ (1024 * 1024));

		if(elapsed >0){
			StringBuffer infoMsg = new StringBuffer();
			infoMsg.append(name)
			.append(" cost=").append(elapsed).append("(ms)")
			.append(",bytesR=").append(totalMBRead).append("(MB)")
			.append(",nR=").append(messagesRead).append("")
			.append(",tps=").append(FORMATTER.format(1000.0 * messagesRead/ (double) elapsed)).append("")
			.append(",avg=").append(avgMBRead).append("(MB/s)");
			
			log(infoMsg.toString(), isEnd);
		} else {
			log("[EF-Msg] No messages received yet", isEnd);
		}

	}
	
	private static void log(String infoMsg, boolean isEnd){
		if (isEnd) {
			LOG.info("############################################# EF-Msg #############################################");
		}
		LOG.error(infoMsg);
		if (isEnd) {
			LOG.info("############################################# EF-Msg #############################################");
		}
	}
}
