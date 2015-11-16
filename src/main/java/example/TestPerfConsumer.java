package example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.easyfun.easyframe.performance.consumer.PerformanceConsumer;

public class TestPerfConsumer {
	public static void main(String[] args) throws Exception{
		List<String> list = new ArrayList<String>();
//		
		list.addAll(Arrays.asList(new String[]{"--topic", "kafkatopic"}));
		list.addAll(Arrays.asList(new String[]{"--zookeeper", "10.3.3.211:2181/kafka"}));
//		list.addAll(Arrays.asList(new String[]{"--zookeeper", "127.0.0.1:2181"}));
//		list.addAll(Arrays.asList(new String[]{"--from-latest"}));
		list.addAll(Arrays.asList(new String[]{"--threads", "2"}));
		list.addAll(Arrays.asList(new String[]{"--show-detailed-stats"}));
		
		PerformanceConsumer.main(list.toArray(new String[0]));
	}
}
