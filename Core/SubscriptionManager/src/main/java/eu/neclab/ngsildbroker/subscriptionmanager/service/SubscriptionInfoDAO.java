package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository
public class SubscriptionInfoDAO extends StorageReaderDAO {
	public Set<String> getAllIds() {
		List<String> tempList = readerJdbcTemplate.queryForList("SELECT id FROM entity", String.class);
		return new HashSet<String>(tempList);
	}
	public Map<String, String> getIds2Type() {
		List<Map<String, Object>> temp = readerJdbcTemplate.queryForList("SELECT id, type FROM entity");
		HashMap<String, String> result = new HashMap<String, String>();
		for(Map<String, Object> entry: temp) {
			result.put(entry.get("id").toString(), entry.get("type").toString());
		}
		return result;
	}
	public String getEntity(String entityId) {
		List<String> tempList = readerJdbcTemplate.queryForList("SELECT data FROM entity WHERE id='" + entityId + "'", String.class);
		return tempList.get(0);
	}
}
