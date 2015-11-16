package com.easyfun.easyframe.msg;

/**
 * 代表消息服务器返回的信息
 * 
 * @author linzhaoming
 *
 * @Created 2014
 */
public class MsgMessage {
	private String message;
	private long offset;
	private int partition;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

}
