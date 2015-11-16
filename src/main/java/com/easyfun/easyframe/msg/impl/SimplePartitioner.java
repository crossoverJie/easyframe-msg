package com.easyfun.easyframe.msg.impl;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/** 
 * 默认配置: partitioner.class=kafka.producer.DefaultPartitioner Key使用Hash value进行分区
 * @author linzhaoming
 * 
 * @Created 2014 */
public class SimplePartitioner implements Partitioner {
	
	public SimplePartitioner(VerifiableProperties prop){
	}

	public int partition(Object key, int numPartitions) {
		if (key instanceof Integer) {
			int partition = 0;
			int iKey = ((Integer) key).intValue();
			if (iKey > 0) {
				partition = iKey % numPartitions;
			}
			return partition;
		} else if (key instanceof String) {
			int abs = abs(key.hashCode());
			return abs % numPartitions;
		}

		return 0;

	}

	private static int abs(int n) {
		return n & 0x7fffffff;
	}

}
