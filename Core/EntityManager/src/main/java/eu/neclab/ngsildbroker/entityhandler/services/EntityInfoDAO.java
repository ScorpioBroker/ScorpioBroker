package eu.neclab.ngsildbroker.entityhandler.services;

import java.util.List;

import org.springframework.stereotype.Repository;
import org.springframework.dao.EmptyResultDataAccessException;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;

@Repository
public class EntityInfoDAO extends StorageDAO {

	public String getEntity(String entityId, String tenantId) throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		result.putAll(AppConstants.INTERNAL_NULL_KEY,
				getJDBCTemplate(null).queryForList("SELECT DISTINCT id FROM entity", String.class));
		List<String> tenants = getTenants();
		for (String tenant : tenants) {
			result.putAll(tenant, getJDBCTemplate(tenant).queryForList("SELECT DISTINCT id FROM entity", String.class));
		}
		if (!result.containsKey(tenantId)) {
			throw new ResponseException(ErrorType.TenantNotFound, "tenant " + tenantId + " not found");
		}
		if (!result.containsValue(entityId)) {
			throw new ResponseException(ErrorType.NotFound, "Entity Id " + entityId + " not found");
		}
		List<String> tempList = getJDBCTemplate(getTenant(tenantId))
				.queryForList("SELECT data FROM entity WHERE id='" + entityId + "'", String.class);
		return tempList.get(0);
	}

	public String getEndpoint(String entityId, String tenantId) throws ResponseException {
		String endpoint = null;
		try {
			endpoint = getJDBCTemplate(getTenant(tenantId)).queryForObject(
					"SELECT endpoint FROM csource cs, csourceinformation csi WHERE cs.id=csi.csource_id AND csi.entity_id='"
							+ entityId + "'",
					String.class);

		} catch (EmptyResultDataAccessException e) {
			return null;
		}
		return endpoint;
	}

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new EntityStorageFunctions();
	}
}
