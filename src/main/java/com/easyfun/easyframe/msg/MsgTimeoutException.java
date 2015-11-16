package com.easyfun.easyframe.msg;

/**
 * 超时消息异常
 * 
 * @author linzhaoming
 *
 * @Created 2014
 */
public class MsgTimeoutException extends RuntimeException {
	public MsgTimeoutException(Exception ex) {
		super(ex);
	}
}
