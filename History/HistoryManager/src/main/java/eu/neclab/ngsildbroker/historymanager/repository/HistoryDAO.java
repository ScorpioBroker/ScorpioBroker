package eu.neclab.ngsildbroker.historymanager.repository;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.storage.TemporalStorageFunctions;

@Repository
public class HistoryDAO extends StorageDAO {

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new TemporalStorageFunctions();
	}

	public boolean entityExists(String entityId, String tenantId) throws ResponseException {
		List<Map<String, Object>> list = getJDBCTemplate(tenantId)
				.queryForList("Select id from temporalentity where id='" + entityId + "';");
		if (list == null || list.isEmpty()) {
			return false;
		}
		return true;
	}

}