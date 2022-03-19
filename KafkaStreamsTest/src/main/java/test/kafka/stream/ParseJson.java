package test.kafka.stream;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import test.kafka.stream.vo.CheckEvent;

public class ParseJson {
	
	/**
	 * Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(ParseJson.class);
	
	// byte array를 string으로 변환 후 특정 값을 key, value list로 리턴 
	public static List<KeyValue<String, String>> getKeyValue(byte[] value) {
		
		CheckEvent event  = new CheckEvent();
		
		// byte array to string
		String valueString = "";
		
		for (int i = 0; i < value.length; i++) {
			
			char valueChar = (char)value[i];
			valueString = valueString + valueChar;
		}
			
		// JSON 타입 파싱 
		try {
			Gson gson = new Gson();
			event = gson.fromJson(valueString, CheckEvent.class);
		} catch (Exception ee) {
			ee.printStackTrace();
		}

		// 요소별 값 list에 추가 
		List<KeyValue<String,String>> list = new ArrayList<>();
		list = setVo(event);
		
//        logger.info("list :" + list);
        return list;
    }
	
	public static List<KeyValue<String, String>> setVo(CheckEvent event) {
		
		Map<String, Object> attrs = event.getData();
		List<KeyValue<String,String>> list = new ArrayList<>();

        for (String key : attrs.keySet()) {
        	
        	BigDecimal val = new BigDecimal(attrs.get(key).toString());	
    		String str = String.valueOf(val);
    		list.add(new KeyValue<>(key, str));

        }
        
        return list;
	}
	
}



