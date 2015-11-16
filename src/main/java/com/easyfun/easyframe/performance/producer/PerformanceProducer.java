package com.easyfun.easyframe.performance.producer;

import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easyfun.easyframe.msg.MsgUtil;

/**
 *  The main program of Producer Perf
 *  
 * 
 * @author linzhaoming
 *
 * @Created 2014-07-26
 */
public class PerformanceProducer {

	private static Log LOG = LogFactory.getLog(PerformanceProducer.class);
	
	private static DecimalFormat FORMATTER = new DecimalFormat("#,##0.00");

	public static void main(String[] args) throws Exception {
		PerfProducerConfig config = new PerfProducerConfig(args);
		
		AtomicLong totalBytesSent = new AtomicLong(0);		//发送字节总数
		AtomicLong totalMessagesSent = new AtomicLong(0);	//发送消息总数
		
		Executor executor = Executors.newFixedThreadPool(config.threads);
				
		CountDownLatch allDone = new CountDownLatch(config.threads); 
		
		long startMs = System.currentTimeMillis();
		
		Runtime.getRuntime().addShutdownHook(new ShutdownThread(startMs, totalBytesSent, totalMessagesSent));
		
		for (int i = 0; i < config.threads; i++) {
			executor.execute(new PerfProducerThread(i, "EF-Msg-Producer-" + i, config, totalBytesSent, totalMessagesSent, allDone));
		}
		allDone.await();// 等待所有子线程执行完
		MsgUtil.closeProducer();
		System.exit(0);
	}
	
	private static class ShutdownThread extends Thread {
		private long startMs;
		private AtomicLong totalBytesSent;
		private AtomicLong totalMessagesSent;

		public ShutdownThread(long startMs, AtomicLong totalBytesSent, AtomicLong totalMessagesSent) {
			this.startMs = startMs;
			this.totalBytesSent = totalBytesSent;
			this.totalMessagesSent = totalMessagesSent;
		}
		
		public void run() {
			long total = System.currentTimeMillis() - startMs;
			
			long sentNum = totalMessagesSent.get();
			if(sentNum >0){
				String avg = FORMATTER.format(total/ sentNum);
				String tps = FORMATTER.format(1000.0 * sentNum/ (double) total);
				String byteSends = FORMATTER.format((totalBytesSent.get() * 1.0) / (1024 * 1024));
				String desc = "[EF-Msg] nSends=" + sentNum + ", byteSends=" + byteSends + "(MB), totalCost=" + total + "(ms), tps=" + tps + "" + ", avg=" + avg + "(ms)";
				LOG.info("############################################# EF-Msg #############################################");
				LOG.error(desc);
				LOG.info("############################################# EF-Msg #############################################");
			}else{
				String desc = "[EF-Msg] No messages sent yet";
				LOG.info("############################################# EF-Msg #############################################");
				LOG.error(desc);
				LOG.info("############################################# EF-Msg #############################################");
			}
			
		}
	}
	
}
