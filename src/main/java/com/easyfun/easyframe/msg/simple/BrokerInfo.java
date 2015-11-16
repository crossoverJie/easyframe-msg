package com.easyfun.easyframe.msg.simple;

import java.io.Serializable;
import com.google.common.base.Objects;

/** 代表一个Broker: host + port
 * 
 * @author linzhaoming
 * 
 * @Created 2014 */
public class BrokerInfo implements Serializable, Comparable<BrokerInfo> {
	public final String host;
	public final int port;

	public BrokerInfo(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public BrokerInfo(String host) {
		this(host, 9092);
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}

	public int hashCode() {
		return Objects.hashCode(host, port);
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		final BrokerInfo other = (BrokerInfo) obj;
		return Objects.equal(this.host, other.host) && Objects.equal(this.port, other.port);
	}

	public String toString() {
		return host + ":" + port;
	}

	/** 从字符串中构建Broker对象, 没有传入端口，默认为9092*/
	public static BrokerInfo fromString(String host) {
		BrokerInfo hp;
		String[] spec = host.split(":");
		if (spec.length == 1) {
			hp = new BrokerInfo(spec[0]);
		} else if (spec.length == 2) {
			hp = new BrokerInfo(spec[0], Integer.parseInt(spec[1]));
		} else {
			throw new IllegalArgumentException("Invalid host specification: " + host);
		}
		return hp;
	}

	public int compareTo(BrokerInfo o) {
		if (this.host.equals(o.host)) {
			return this.port - o.port;
		} else {
			return this.host.compareTo(o.host);
		}
	}
}
