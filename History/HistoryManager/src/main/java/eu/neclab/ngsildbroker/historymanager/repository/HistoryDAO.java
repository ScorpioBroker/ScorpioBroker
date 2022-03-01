package eu.neclab.ngsildbroker.historymanager.repository;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Repository;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
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

	public void entityExists(String entityId, String tenantId) throws ResponseException {
		List<Map<String, Object>> list = getJDBCTemplate(tenantId)
				.queryForList("Select id from temporalentity where id='" + entityId + "';");
		if (list.size() > 0 || !list.isEmpty()) {
			throw new ResponseException(ErrorType.AlreadyExists,entityId + " already exists");
		}
	}
	
	public void getAllIds(String entityId, String tenantId) throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		result.putAll(AppConstants.INTERNAL_NULL_KEY,
				getJDBCTemplate(null).queryForList("SELECT DISTINCT id FROM temporalentity", String.class));
		List<String> tenants = getTenants();
		for (String tenant : tenants) {
			result.putAll(tenant,
					getJDBCTemplate(tenant).queryForList("SELECT DISTINCT id FROM temporalentity", String.class));
		}
		if (!result.containsValue(entityId)) {
			throw new ResponseException(ErrorType.NotFound, "Entity Id " + entityId + " not found");
		}
		if (!result.containsKey(tenantId)) {
			throw new ResponseException(ErrorType.TenantNotFound, "tenant " + tenantId + " not found");
		}
	}
}