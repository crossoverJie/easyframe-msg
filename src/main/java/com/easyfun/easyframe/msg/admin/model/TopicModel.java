package com.easyfun.easyframe.msg.admin.model;

import java.util.List;

public class TopicModel {
	private String name;
	private List<PartitionModel> partitions;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<PartitionModel> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<PartitionModel> partitions) {
		this.partitions = partitions;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Topic: name=(" + name + ")");
		sb.append(", partititons=(\n");
		for (PartitionModel model : partitions) {
			sb.append(model).append("\n");
		}
		sb.append("\n");
//		return "Topic: name=(" + name + "), partitions=(" + partitions + ")";
		return sb.toString();
	}
}
