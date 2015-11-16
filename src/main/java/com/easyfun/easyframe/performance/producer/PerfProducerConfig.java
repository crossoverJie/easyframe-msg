package com.easyfun.easyframe.performance.producer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Config for Performace Perf.
 * 
 * <p>The supporting options are:
 * <li>--broker-list: [REQUIRED] The *addresses* of broker lists, such as \"localhost:9082\".
 * <li>--topics: [REQUIRED]: The comma separated list of topics to produce to.
 * <li>--msg-num: The number of messages to send. Default is 10.
 * <li>--msg-prefix: The *prefix* of message to send. Default is EasyFrameMsg.
 * <li>--interval: Interval at which to print progress info. Default is 1000
 * <li>-threads: Number of sending threads. Default is 1
 * 
 * @author linzhaoming
 *
 * @Created 2014-07-26
 */
public class PerfProducerConfig {
	/** BrokerList地址*/
	String brokerList;
	/** Topic列表 */
	String[] topics;
	/** 发送的消息总数*/
	long msgNum;
	/** 消息内容前缀*/
	String msgPrefix;
	/** 显示提示信息的发送消息间隔数量*/
	int interval;
	/** 并发执行线程数*/
	int threads;
	
	long messagesPerThread;
	
	private OptionParser parser;
	
	private static Log LOG = LogFactory.getLog(PerfProducerConfig.class);
	
	public PerfProducerConfig(String[] args) throws Exception{
		//解析函数
		parser = new OptionParser();
		
		//broker-list
		OptionSpec brokerListOpt = parser.accepts("broker-list", "[REQUIRED] The *addresses* of broker lists, such as \"localhost:9082\".")
			    .withRequiredArg()
			    .describedAs("hostname:port,hostname:port")
			    .ofType(String.class);
		
		//topics
		OptionSpec topicsOpt = parser.accepts("topics", "[REQUIRED]: The comma separated list of topics to produce to.")
	    	      .withRequiredArg()
	    	      .describedAs("topic1,topic2..")
	    	      .ofType(String.class);
		
		//msg_num
		OptionSpec numMessagesOpt = parser.accepts("msg-num", "The number of messages to send.")
				    .withRequiredArg()
				    .describedAs("count")
				    .ofType(Long.class)
				    .defaultsTo(10L);
		
		//msg_prefix
		OptionSpec msgPrefixOpt = parser.accepts("msg-prefix", "The *prefix* of message to send.")
			    .withRequiredArg()
			    .describedAs("count")
			    .ofType(String.class)
			    .defaultsTo("EasyFrameMsg");
		
		//interval
		OptionSpec reportingIntervalOpt = parser.accepts("interval", "Interval at which to print progress info.")
				    .withRequiredArg()
				    .describedAs("size")
				    .ofType(Integer.class)
				    .defaultsTo(10000);
		
		//threads
		OptionSpec numThreadsOpt = parser.accepts("threads", "Number of sending threads.")
	            .withRequiredArg()
	            .describedAs("number of threads")
	            .ofType(Integer.class)
	            .defaultsTo(1);
		
		
		//检查必需参数
		OptionSet options = parser.parse(args);
		List<OptionSpec> list = new ArrayList<OptionSpec>();
		list.add(topicsOpt);
		list.add(brokerListOpt);
		list.add(numMessagesOpt);
		for (OptionSpec arg : list) {
			if (!options.has(arg)) {
				System.err.println("Missing required argument \"" + arg + "\"");
				parser.printHelpOn(System.err);
				System.exit(1);
			}
		}

		this.brokerList = options.valueOf(brokerListOpt).toString();
		System.setProperty("producer.metadata.broker.list", this.brokerList);
		
		System.setProperty("producer.serializer.class", "kafka.serializer.StringEncoder");
		
		String topicsStr = options.valueOf(topicsOpt).toString();
		this.topics = topicsStr.split(",");
		this.msgNum = Long.valueOf(options.valueOf(numMessagesOpt).toString()).longValue();
		this.msgPrefix = options.valueOf(msgPrefixOpt).toString();
		this.interval = Integer.valueOf(options.valueOf(reportingIntervalOpt).toString()).intValue();
		this.threads =  Integer.valueOf(options.valueOf(numThreadsOpt).toString()).intValue();
		
		messagesPerThread = msgNum / threads;
		debug("Messages per thread = " + messagesPerThread);
	}
	
	private static void debug(String str){
		if(LOG.isDebugEnabled()){
			LOG.debug(str);
		}
	}
}
