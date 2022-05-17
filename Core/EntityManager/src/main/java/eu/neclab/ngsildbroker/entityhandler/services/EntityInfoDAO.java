package eu.neclab.ngsildbroker.entityhandler.services;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;

@Repository
public class EntityInfoDAO extends StorageDAO {

	public ArrayListMultimap<String, String> getAllIds() throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		result.putAll(AppConstants.INTERNAL_NULL_KEY,
				getJDBCTemplate(null).queryForList("SELECT DISTINCT id FROM entity", String.class));
		List<String> tenants = getTenants();
		for (String tenant : tenants) {
			result.putAll(tenant, getJDBCTemplate(tenant).queryForList("SELECT DISTINCT id FROM entity", String.class));
		}

		return result;
	}

	public String getEntity(String entityId, String tenantId) throws ResponseException {
		List<String> tempList = getJDBCTemplate(getTenant(tenantId))
				.queryForList("SELECT data FROM entity WHERE id='" + entityId + "'", String.class);
		return tempList.get(0);
	}

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new EntityStorageFunctions();
	}
}
