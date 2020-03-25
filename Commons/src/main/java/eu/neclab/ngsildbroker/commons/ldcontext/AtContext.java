package eu.neclab.ngsildbroker.commons.ldcontext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;

import eu.neclab.ngsildbroker.commons.constants.KafkaConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;


@Component
public class AtContext {
	
	
	private HashMap<String, List<Object>> id2Contextes = new HashMap<String, List<Object>>();
	private Gson gson = new Gson();
	

	@Autowired
	KafkaOps kafkaOps;
	
	@Autowired
	AtContextProducerChannel producerChannel;
	
	
	@SuppressWarnings("unchecked")
	@PostConstruct
	private void loadMap() {
		Map<String, byte[]> contextes = kafkaOps.pullFromKafka(KafkaConstants.ATCONTEXT_TOPIC);
		for(Entry<String, byte[]> entry: contextes.entrySet()) {
			this.id2Contextes.put(entry.getKey(), gson.fromJson(new String(entry.getValue()), List.class));
		}
	}
	
	public void addContext(String id, List<Object> context) throws ResponseException {
		
		this.id2Contextes.put(id, context);
		saveContext(id);
	}
	
	public Map<String, byte[]> getAllContextes() {
		return kafkaOps.pullFromKafka(KafkaConstants.ATCONTEXT_TOPIC);
	}
	@SuppressWarnings("unchecked")
	public List<Object> getContextes(String id){
//		byte[] kafkaBytes = kafkaOps.pullFromKafka(KafkaConstants.ATCONTEXT_TOPIC).get(id);
//		if(kafkaBytes == null || kafkaBytes == AppConstants.NULL_BYTES) {
//			return new HashMap<String, Object>();
//		}else {
//			return gson.fromJson(new String(kafkaBytes), Map.class);
//		}
		
		return this.id2Contextes.get(id);
	}

	
	
	private void saveContext(String id) throws ResponseException {
		kafkaOps.pushToKafka(producerChannel.atContextWriteChannel(), id.getBytes(), gson.toJson(id2Contextes.get(id)).getBytes());
		
	}

	@KafkaListener(topics = "ATCONTEXT", groupId = "atCon")
	public void listenContext(Message<byte[]> message) {
		List<Object> context = gson.fromJson(new String(message.getPayload()), List.class);
		String key = kafkaOps.getMessageKey(message);
		id2Contextes.put(key, context);
	}
}
