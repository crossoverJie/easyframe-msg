package com.easyfun.easyframe.performance.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.utils.ZkUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Config for Consumer Perf.
 * 
 * <p>The supporting options are:
 * <li>--messages: The number of messges to consume. Default is Long.MAX_VALUE
 * <li>--interval: Interval at which to print progress info
 * <li>--show-detailed-stats: If set, stats are reported for each reporting interval as configured by interval
 * <li>--zookeeper: REQUIRED: The connection string for the zookeeper connection in the form host:port.
 * <li>--topic: REQUIRED: The topic to consume from.
 * <li>--group: The group id to consume on. Default is "EasyFrame-perf-consumer-[Random]"
 * <li>--fetch-size: The amount of data to fetch in a single request. Default is 1024 * 1024
 * <li>--from-latest: If the consumer does not already have an established offset to consume from, start with the latest message present in the log rather than the earliest message.
 * <li>--socket-buffer-size: The size of the tcp RECV size. Default is 2M
 * <li>--threads: Number of processing threads. Default is 10
 * <li>--num-fetch-threads: Number of fetcher threads. Default is 1
 * 
 * @author linzhaoming
 *
 * @Created 2014-07-26
 */
class PerfConsumerConfig {
	int numThreads;
	String topic;
	long numMessages;
	int reportingInterval;	
	boolean showDetailedStats;
	
	
	private boolean hasGroupId = false;
	private String groupId;
	private String zkConnect;
	
	private OptionParser parser;
	
	private static Log LOG = LogFactory.getLog(PerfConsumerConfig.class);
	
	public PerfConsumerConfig(String[] args) throws Exception{
		//解析函数
		parser = new OptionParser();
		
		OptionSpec numMessagesOpt = parser.accepts("messages", "The number of messages to send or consume.")
				    .withRequiredArg()
				    .describedAs("count")
				    .ofType(Long.class)
				    .defaultsTo(Long.MAX_VALUE);
		
		OptionSpec reportingIntervalOpt = parser.accepts("interval", "Interval at which to print progress info.")
				    .withRequiredArg()
				    .describedAs("size")
				    .ofType(Integer.class)
				    .defaultsTo(10000);
		
		OptionSpec showDetailedStatsOpt = parser.accepts("show-detailed-stats", "If set, stats are reported for each reporting " +
			    "interval as configured by interval.");
		
		//zookeeper.connect
		OptionSpec zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " + "Multiple URLS can be given to allow fail-over.")
			    .withRequiredArg()
			    .describedAs("hostname:port,hostname:port")
			    .ofType(String.class);
		
		OptionSpec topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
	    	      .withRequiredArg()
	    	      .describedAs("topic")
	    	      .ofType(String.class);
		
		OptionSpec groupIdOpt = parser.accepts("group", "The group id to consume on.")
                .withRequiredArg()
                .describedAs("gid")
                .defaultsTo("EasyFrame-perf-consumer-" + new Random().nextInt(100000))
                .ofType(String.class);
		
		OptionSpec fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(1024 * 1024);
		
		OptionSpec resetBeginningOffsetOpt = parser.accepts("from-latest", "If the consumer does not already have an established " +
			      "offset to consume from, start with the latest message present in the log rather than the earliest message.");
		
		OptionSpec socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                 .withRequiredArg()
                 .describedAs("size")
                 .ofType(Integer.class)
                 .defaultsTo(2 * 1024 * 1024);
		
		OptionSpec numThreadsOpt = parser.accepts("threads", "Number of processing threads.")
                 .withRequiredArg()
                 .describedAs("count")
                 .ofType(Integer.class)
                 .defaultsTo(10);
		
		OptionSpec numFetchersOpt = parser.accepts("num-fetch-threads", "Number of fetcher threads.")
                     .withRequiredArg()
                     .describedAs("count")
                     .ofType(Integer.class)
                     .defaultsTo(1);
		
		//检查必需参数
		OptionSet options = parser.parse(args);
		List<OptionSpec> list = new ArrayList<OptionSpec>();
		list.add(topicOpt);
		list.add(zkConnectOpt);
		for (OptionSpec arg : list) {
			if (!options.has(arg)) {
				System.err.println("Missing required argument \"" + arg + "\"");
				parser.printHelpOn(System.err);
				System.exit(1);
			}
		}
		
		handleOpt("consumer.group.id", options.valueOf(groupIdOpt));
		handleOpt("consumer.socket.receive.buffer.bytes", options.valueOf(socketBufferSizeOpt).toString());
		handleOpt("consumer.fetch.message.max.bytes", options.valueOf(fetchSizeOpt).toString());
		handleOpt("consumer.auto.offset.reset", options.has(resetBeginningOffsetOpt) ? "largest" : "smallest");
		handleOpt("consumer.zookeeper.connect", options.valueOf(zkConnectOpt));
//		handleOpt("consumer.consumer.timeout.ms", "5000");
		handleOpt("consumer.consumer.timeout.ms", "-1");	//否则会抛TimeOutException
		handleOpt("consumer.num.consumer.fetchers", options.valueOf(numFetchersOpt).toString());
		
		numThreads = Integer.valueOf(options.valueOf(numThreadsOpt).toString()).intValue();
		topic = options.valueOf(topicOpt).toString();
		numMessages = Long.valueOf(options.valueOf(numMessagesOpt).toString()).longValue();
		reportingInterval = Integer.valueOf(options.valueOf(reportingIntervalOpt).toString()).intValue();
		showDetailedStats = options.has(showDetailedStatsOpt);
		
		hasGroupId = options.has(groupIdOpt);
		groupId = options.valueOf(groupIdOpt).toString();
		zkConnect = options.valueOf(zkConnectOpt).toString();
	}
	
	/** If group is not set, clear the Group Id offset when shutting down*/
	public void clearConsumer() {
		if (!hasGroupId) {			
			ZkUtils.maybeDeletePath(zkConnect, "/consumers/" + groupId);
			debug("Clearing ZK Offset infor: [" + groupId + "]");
		}
	}
	
	protected static void debug(String str){
		if(LOG.isDebugEnabled()){
			LOG.debug(str);
		}
	}
	
	private void handleOpt(String key, Object value){
		if(StringUtils.isNotBlank(value.toString())){
			System.setProperty(key, value.toString());
		}
	}
}
