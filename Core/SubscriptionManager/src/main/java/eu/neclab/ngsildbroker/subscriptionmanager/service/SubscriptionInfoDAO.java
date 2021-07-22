package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Repository;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository
public class SubscriptionInfoDAO extends StorageReaderDAO {
	private final static Logger logger = LogManager.getLogger(SubscriptionInfoDAO.class);

	public Set<String> getAllIds(String tenantId) throws ResponseException {
		List<String> tempList = getJDBCTemplate(tenantId).queryForList("SELECT id FROM entity", String.class);
		return new HashSet<String>(tempList);
	}

	public Table<String, String, String> getIds2Type() throws ResponseException {
		Table<String, String, String> result = HashBasedTable.create();
		for (Map<String, Object> entry : getJDBCTemplate(null).queryForList("SELECT id, type FROM entity")) {
			result.put(AppConstants.INTERNAL_NULL_KEY, entry.get("id").toString(), entry.get("type").toString());
		}
		List<String> tenants = getTenants();
		for (String tenantId : tenants) {
			tenantId = getTenant(tenantId);
			List<Map<String, Object>> temp = getJDBCTemplate(tenantId).queryForList("SELECT id, type FROM entity");
			for (Map<String, Object> entry : temp) {
				result.put(tenantId, entry.get("id").toString(), entry.get("type").toString());
			}

		}
		return result;
	}

	public String getEntity(String entityId, String tenantId) {
		tenantId = getTenant(tenantId);
		QueryParams qp = new QueryParams();
		qp.setId(entityId);
		qp.setTenant(tenantId);
		try {
			return super.query(qp).get(0);
		} catch (ResponseException e) {
			logger.info("Failed to get info for entity with id " + entityId);
			logger.debug(e);
			return null;
		}
	}
}
