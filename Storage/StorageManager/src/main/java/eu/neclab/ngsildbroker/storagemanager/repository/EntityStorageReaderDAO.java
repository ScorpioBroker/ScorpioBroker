package eu.neclab.ngsildbroker.storagemanager.repository;

import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository
@ConditionalOnProperty(value="reader.enabled", havingValue = "true", matchIfMissing = false)
public class EntityStorageReaderDAO extends StorageReaderDAO {
	
	public Long getLocalEntitiesCount(String tenantId) throws ResponseException {
		List<Map<String, Object>> list = getJDBCTemplate(tenantId).queryForList(
				"SELECT count(id) FROM entity;");
		if(list == null ||list.isEmpty()) {
			return null;
		}
		return (Long) list.get(0).get("count");

	}
	public Long getLocalTypesCount(String tenantId) throws ResponseException {
		List<Map<String, Object>> list = getJDBCTemplate(tenantId).queryForList(
				"SELECT count(distinct(type)) FROM entity;");
		if(list == null ||list.isEmpty()) {
			return null;
		}
		return (Long) list.get(0).get("count");

	}
}
