package example;

import java.util.ArrayList;
import java.util.List;

import com.easyfun.easyframe.msg.MsgUtil;

/** 测试程序：Producer
 * @author linzhaoming
 * 
 * @Created 2014年7月8日  */
public class TestProducer {
	public static void main(String[] args) {
		//1、发送指定topic名字的Topic
		String topicName = "linzm";
		for (int i = 0; i < 100; i++) {
			MsgUtil.send(topicName, "Message_" + i);
		}
		
		//2. 发送默认topic名字的Topic
		for (int i = 0; i < 100; i++) {
			MsgUtil.send("Message_" + i);
		}
		
		List list = new ArrayList();
		for(int i=0; i<10; i++){
			list.add("test_" + i);
		}
		
		//3. 批量发送Topic(指定Topic)		
		MsgUtil.send(topicName, list);
		
		//4. 批量发送Topic(默认Topic)
		MsgUtil.send(list);
		
		//关闭Producer
		MsgUtil.closeProducer();
	}
}
