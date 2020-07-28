package eu.neclab.ngsildbroker.subscriptionmanager.service;

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
	public Map<String, Object> getIds2Type() {
		Map<String, Object> result = readerJdbcTemplate.queryForMap("SELECT id, type FROM entity");
		return result;
	}
	public String getEntity(String entityId) {
		List<String> tempList = readerJdbcTemplate.queryForList("SELECT data FROM entity WHERE id='" + entityId + "'", String.class);
		return tempList.get(0);
	}
}
