package com.easyfun.easyframe.msg.admin.model;

public class BrokerModel {
	private String host;
	private int id;
	private int port;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String toString() {
		return "Broker: id=(" + id + "), host=(" + host + "), port=(" + port + ")";
	}
}
