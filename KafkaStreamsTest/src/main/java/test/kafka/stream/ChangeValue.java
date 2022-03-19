package test.kafka.stream;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeValue {

	private static final Logger logger = LoggerFactory.getLogger(ChangeValue.class);
	
	public static List<KeyValue<String, String>> reValue(String num, String value) {
		
		switch(num) {
		case "1" : value = "1"; break;
		case "2" : value = "2"; break; 		
		case "3" : value = "3"; break; 
		}
		
		List<KeyValue<String,String>> list = new ArrayList<>();
		list.add(new KeyValue<>(num, value));
		
		return list;
	}
}
