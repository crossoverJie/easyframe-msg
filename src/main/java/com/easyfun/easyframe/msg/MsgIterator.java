package com.easyfun.easyframe.msg;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;

/** As the result <code>Iterator</code> of Consumer
 * 
 * @author linzhaoming
 * 
 * @Created 2014-07-08 */
public class MsgIterator {
	private ConsumerIterator<byte[], byte[]> iter = null;

	public MsgIterator(ConsumerIterator<byte[], byte[]> iter) {
		this.iter = iter;
	}

	/** 判断是否有下一条消息
	 * @return */
	public boolean hasNext() throws MsgTimeoutException{
		try {
			return iter.hasNext();
		} catch (ConsumerTimeoutException ex) {
			// 超时并不意味着出错，只是暂时没有消息
			throw new MsgTimeoutException(ex);
		}
	}

	/** 获取下一条消息(基本消息内容)
	 * @return */
	public String next() {
		return new String(iter.next().message());
	}
	
	/** 高级用法: 获取下一条消息(包括message/offset/partition的信息)
	 * 
	 * <li>注意事项：next()与advanceNext()调用了之后，指针就迁移
	 * @return */
	public MsgMessage advanceNext() {
		MessageAndMetadata<byte[], byte[]> next = iter.next();
		MsgMessage msg = new MsgMessage();
		msg.setMessage(new String(next.message()));
		msg.setOffset(next.offset());
		msg.setPartition(next.partition());
		return msg;
	}
	
	public ConsumerIterator<byte[], byte[]> getIter(){
		return iter;
	}
}
