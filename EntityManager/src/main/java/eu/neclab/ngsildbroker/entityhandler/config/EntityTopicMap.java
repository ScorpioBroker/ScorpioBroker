package eu.neclab.ngsildbroker.entityhandler.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import eu.neclab.ngsildbroker.commons.datatypes.EntityDetails;


public class EntityTopicMap {
	
	public EntityTopicMap(){}
	
	private static Map<String, EntityDetails> topicMap = new ConcurrentHashMap<String, EntityDetails>(1000);

	public void put(String key, EntityDetails details) {
		topicMap.put(key, details);
	}

	public EntityDetails get(String key) {
		return topicMap.get(key);
	}
	
	public boolean isExist(String key) {
		return topicMap.containsKey(key);
	}
	
	public void remove(String key) {
		topicMap.remove(key);
	}

}
