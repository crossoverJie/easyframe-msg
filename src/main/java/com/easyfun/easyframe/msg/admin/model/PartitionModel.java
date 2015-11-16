package com.easyfun.easyframe.msg.admin.model;

import java.util.List;

/**
 * 代表Partition信息
 * 
 * @author linzhaoming
 *
 * @Created 2014
 */
public class PartitionModel {
	private int id;

	private int leader;

	private int epoch;

	private List<Integer> syncs;

	private List<Integer> replicass;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getLeader() {
		return leader;
	}

	public void setLeader(int leader) {
		this.leader = leader;
	}

	public int getEpoch() {
		return epoch;
	}

	public void setEpoch(int epoch) {
		this.epoch = epoch;
	}

	public List<Integer> getSyncs() {
		return syncs;
	}

	public void setSyncs(List<Integer> syncs) {
		this.syncs = syncs;
	}

	public List<Integer> getReplicass() {
		return replicass;
	}

	public void setReplicass(List<Integer> replicass) {
		this.replicass = replicass;
	}
	
	public String toString() {
		return "Partition: id=(" + id + "), leader=(" + leader + "), epoch=(" + epoch + "), syncs=(" + syncs + "), relicas=(" + replicass + ")";
	}

}
