package eu.neclab.ngsildbroker.historymanager.repository;

import java.sql.ResultSet;
import java.util.HashMap;
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

		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		if (tenantId == AppConstants.INTERNAL_NULL_KEY) {
			result.putAll(AppConstants.INTERNAL_NULL_KEY,
					getJDBCTemplate(null).queryForList("SELECT DISTINCT id FROM temporalentity", String.class));
			if (result.containsValue(entityId)) {
				throw new ResponseException(ErrorType.AlreadyExists, entityId + " already exists");
			}
		} else {

			result.putAll(tenantId,
					getJDBCTemplate(tenantId).queryForList("SELECT DISTINCT id FROM temporalentity", String.class));
			if (result.containsValue(entityId)) {
				throw new ResponseException(ErrorType.AlreadyExists, entityId + " already exists");
			}

		}
	}

	public void attributeExists(String entityId, String tenantId, String resolvedAttrId, String instanceId)
			throws ResponseException {
		Map<String, String> results = new HashMap<>();
		if (tenantId == AppConstants.INTERNAL_NULL_KEY) {
			getJDBCTemplate(null)
					.query("select attributeid, instanceid from temporalentityattrinstance WHERE temporalentity_id='"
							+ entityId + "' and attributeid='" + resolvedAttrId + "'", (ResultSet rs) -> {
								while (rs.next()) {
									results.put(rs.getString("instanceid"), rs.getString("attributeid"));
								}
								return true;
							});
			if (results.isEmpty()) {
				throw new ResponseException(ErrorType.NotFound, resolvedAttrId + " not found");
			}
			if (instanceId != null) {
				if (!results.containsKey(instanceId)) {
					throw new ResponseException(ErrorType.NotFound, instanceId + " not found");
				}
			}
		} else {
			getJDBCTemplate(tenantId)
					.query("select attributeid, instanceid from temporalentityattrinstance WHERE temporalentity_id='"
							+ entityId + "' and attributeid='" + resolvedAttrId + "'", (ResultSet rs) -> {
								while (rs.next()) {
									results.put(rs.getString("instanceid"), rs.getString("attributeid"));
								}
								return true;
							});
			if (results.isEmpty()) {
				throw new ResponseException(ErrorType.NotFound, resolvedAttrId + " not found");
			}
			if (instanceId != null) {
				if (!results.containsKey(instanceId)) {
					throw new ResponseException(ErrorType.NotFound, instanceId + " not found");
				}
			}

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