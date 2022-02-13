package eu.neclab.ngsildbroker.historymanager.repository;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.storage.TemporalStorageFunctions;


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
	
	public ArrayListMultimap<String, String> getAllIds() throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		result.putAll(AppConstants.INTERNAL_NULL_KEY,
				getJDBCTemplate(null).queryForList("SELECT DISTINCT id FROM temporalentity", String.class));
		List<String> tenants = getTenants();
		for (String tenant : tenants) {
			result.putAll(tenant, getJDBCTemplate(tenant).queryForList("SELECT DISTINCT id FROM temporalentity", String.class));
		}

		return result;
	}

}