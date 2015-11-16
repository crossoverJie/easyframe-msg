package com.easyfun.easyframe.performance.producer;

import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easyfun.easyframe.msg.MsgUtil;

/**
 * The Thread to perform the Producer perf
 * 
 * @author linzhaoming
 *
 * @Created 2014-07-26
 */
public class PerfProducerThread extends Thread {
	private static Log LOG = LogFactory.getLog(PerfProducerThread.class);
	
	private CountDownLatch allDone;
	private PerfProducerConfig config = null;
	
	private String name;
	private int threadId ;
	
	private AtomicLong totalBytesSent;
	private AtomicLong totalMessagesSent;
	
	private static final String SPLIT = ":";
	
	private static DecimalFormat FORMATTER = new DecimalFormat("#,##0.00");
	
	public PerfProducerThread(int threadId, String name, PerfProducerConfig config, AtomicLong totalBytesSent, AtomicLong totalMessagesSent, CountDownLatch allDone) {
		this.name = name;
		this.threadId = threadId;
		this.config = config;
		this.totalBytesSent = totalBytesSent;
		this.totalMessagesSent = totalMessagesSent;
		this.allDone = allDone;
	}
	
	public void run() {
		Thread.currentThread().setName(name);
		long bytesSent = 0;		//当前线程已发送字节数统计
		int nSends = 0;			//当前线程已发送消息数量统计
		
		String msgPrefix = Thread.currentThread().getName() + SPLIT + config.msgPrefix+ SPLIT;
		long seqPrefix =  config.messagesPerThread * threadId;	//线程的开始顺序号
		 
		//实时统计消息发送延时
		long maxLatency = -1L;	
		long totalLatency = 0;
		for (int i = 0; i < config.messagesPerThread; i++) {
			String msg = msgPrefix + (seqPrefix + i);
			debug(msg);
			
			for (String topic : config.topics) {
				long sendStart = System.currentTimeMillis();
				MsgUtil.send(topic, msg);
				
				long sendEllapsed = System.currentTimeMillis() - sendStart;
				maxLatency = Math.max(maxLatency, sendEllapsed);
				totalLatency += sendEllapsed;
				
				int length = msg.getBytes().length;
				bytesSent += length;
				nSends ++;
				
				totalMessagesSent.addAndGet(1);
				totalBytesSent.addAndGet(length);
			}
			
			if (i % config.interval == 0) {
				String byteSends = FORMATTER.format((bytesSent * 1.0) / (1024 * 1024));
				StringBuffer infoMsg = new StringBuffer();
				infoMsg.append(Thread.currentThread().getName()).append("-").append(i)
				.append(", max=").append(maxLatency).append("(ms)")
				.append(", avg=").append(FORMATTER.format((totalLatency / (double) config.interval))).append("(ms)")
				.append(", bytesSends=").append(byteSends).append("(MB)")
				.append(", nSends=(").append(nSends).append(")");
				
				LOG.info(infoMsg.toString());
				totalLatency = 0L;
				maxLatency = -1L;
			}
		}
		
		totalBytesSent.addAndGet(bytesSent);

		this.allDone.countDown();
	}
	
	private static void debug(String str){
		if(LOG.isDebugEnabled()){
			LOG.debug(str);
		}
	}
}
