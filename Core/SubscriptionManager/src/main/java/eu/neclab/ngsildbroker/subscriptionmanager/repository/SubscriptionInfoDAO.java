package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
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
		Map<String, String> entityInfo = new HashMap<String, String>();
		entityInfo.put(NGSIConstants.JSON_LD_ID, entityId);
		List<Map<String, String>> temp = new ArrayList<Map<String, String>>();
		temp.add(entityInfo);
		qp.setEntities(temp);
		qp.setTenant(tenantId);
		try {
			return super.query(qp).getActualDataString().get(0);
		} catch (ResponseException e) {
			logger.info("Failed to get info for entity with id " + entityId);
			logger.debug(e);
			return null;
		}
	}

	public List<String> getStoredSubscriptions() {
		List<String> tenants;
		tenants = getTenants();
		ArrayList<String> result = new ArrayList<String>();
		try {
			result.addAll(
					getJDBCTemplate(null).queryForList("SELECT subscription_request FROM subscriptions", String.class));
			for (String tenantId : tenants) {
				tenantId = getTenant(tenantId);
				result.addAll(getJDBCTemplate(tenantId).queryForList("SELECT subscription_request FROM subscriptions",
						String.class));
			}
		} catch (DataAccessException | ResponseException e) {
			logger.error("Subscriptions could not be loaded", e);
		}
		return result;
	}

	public void storedSubscriptions(Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription) {
		Set<String> tenants = tenant2subscriptionId2Subscription.rowKeySet();
		try {
			for (String tenant : tenants) {
				Map<String, SubscriptionRequest> row = tenant2subscriptionId2Subscription.row(tenant);
				tenant = getTenant(tenant);
				JdbcTemplate template = getJDBCTemplate(tenant);
				template.execute("DELETE FROM subscriptions");
				for (Entry<String, SubscriptionRequest> entry : row.entrySet()) {
					template.update("INSERT INTO subscriptions (subscription_id, subscription_request) VALUES (?, ?)",
							entry.getKey(), DataSerializer.toJson(entry.getValue()));
				}
			}
		} catch (DataAccessException | ResponseException e) {
			logger.error("Subscriptions could not be loaded", e);
		}

	}
}
