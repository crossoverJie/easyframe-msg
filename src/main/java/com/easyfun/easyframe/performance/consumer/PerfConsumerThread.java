package com.easyfun.easyframe.performance.consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easyfun.easyframe.msg.MsgIterator;

/**
 * The Thread to perform the Consumer perf
 * 
 * @author linzhaoming
 *
 * @Created 2014-07-26
 */
class PerfConsumerThread extends Thread {
	private static Log LOG = LogFactory.getLog(PerfConsumerThread.class);
	
	private CountDownLatch shutdownLatch = new CountDownLatch(1);

	int threadId;
	AtomicLong totalMessagesRead;
	AtomicLong totalBytesRead;
	PerfConsumerConfig config;
	MsgIterator msgIter;

	public PerfConsumerThread(int threadId, String name, MsgIterator msgIter, PerfConsumerConfig config, AtomicLong totalMessagesRead,
			AtomicLong totalBytesRead) {
		super(name);
		this.threadId = threadId;
		this.msgIter = msgIter;
		this.config = config;
		this.totalMessagesRead = totalMessagesRead;
		this.totalBytesRead = totalBytesRead;
	}

	public void shutdown() throws Exception {
		shutdownLatch.await();
	}

	public void run() {
		long bytesRead = 0L;	//线程读取的总字节数
		long messagesRead = 0L;	//线程读取的消息数
		long startMs = System.currentTimeMillis();

		while (msgIter.hasNext()) {
			if (messagesRead < config.numMessages) {
				messagesRead += 1;
				String message = msgIter.next();
				LOG.debug(Thread.currentThread().getName() + " received [" + message + "]");;

				int length = message.getBytes().length;
				bytesRead += length;
				
				totalMessagesRead.incrementAndGet();
				totalBytesRead.addAndGet(length);

				if (messagesRead % config.reportingInterval == 0) {
					if (config.showDetailedStats) {
						PerformanceConsumer.printMessage(Thread.currentThread().getName(), bytesRead, messagesRead, (System.currentTimeMillis() -startMs), false);
					}
				}
			}
		}
		
		if (config.showDetailedStats) {
			PerformanceConsumer.printMessage(Thread.currentThread().getName(), bytesRead, messagesRead, (System.currentTimeMillis() -startMs), false);
		}
		
		shutdownComplete();
	}

	private void shutdownComplete() {
		shutdownLatch.countDown();
	}
}
