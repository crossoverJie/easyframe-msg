package example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.easyfun.easyframe.performance.producer.PerformanceProducer;

public class TestPerfProducer {
	public static void main(String[] args) throws Exception {
		List<String> list = new ArrayList<String>();
		
		list.addAll(Arrays.asList(new String[]{"--broker-list", "10.3.3.211:39092"}));
//		list.addAll(Arrays.asList(new String[]{"--broker-list", "127.0.0.1:39092"}));
		list.addAll(Arrays.asList(new String[]{"--topics", "kafkatopic"}));
		list.addAll(Arrays.asList(new String[]{"--msg-num", "10000"}));
		list.addAll(Arrays.asList(new String[]{"--threads", "1"}));
		
		PerformanceProducer.main(list.toArray(new String[0]));
	}
}
