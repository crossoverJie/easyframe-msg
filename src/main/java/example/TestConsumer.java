package example;

import com.easyfun.easyframe.msg.MsgIterator;
import com.easyfun.easyframe.msg.MsgUtil;

/** 测试程序：Consumer
 * 
 * @author linzhaoming
 *
 * @Created 2014
 */
public class TestConsumer {
	public static void main(String[] args) {
		//消费消息 指定Topic
		String topicName = "linzm";
		MsgIterator iter = MsgUtil.consume(topicName);

		while (true) {
			if (iter.hasNext()) {
				System.out.println(iter.next());
			} else {
				continue;
			}
		}
		
		//消费消息 [默认Topic]
		
	}
}
